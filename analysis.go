package main

import (
	"flag"
	"time"
	"github.com/sirupsen/logrus"
	"os"
	"bufio"
	"io"
	"strings"
	"github.com/mgutz/str"  //封装了string库
	"net/url"
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"github.com/mediocregopher/radix.v2/pool"
	"fmt"
)

const HANDLE_DIG  = " /dig?"
const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST  = "/list/"
const HANDLE_HTML  = ".html"
const REDIS_HOST  = "39.107.77.94"
const REDIS_PORT  = 6379
var REDIS_URL  = REDIS_HOST + ":" + strconv.Itoa(REDIS_PORT)

//命令行配置参数
type cmdParams struct {
	logFilePath string
	routineNum int
}
// 当前页面附加数据
type digData struct {
	time string
	url string
	refer string
	ua string
}

type urlData struct {
	data digData
	uid string  //uv统计时去重，来源于客户端ID
	unode urlNode
}
// 当前页面基本数据
type urlNode struct {
	unType string  //movie  list  /home 这些页面
	unRid int  //Resource ID 资源ID
	unUrl string //当前页面的URL
	unTime string   //当前访问这个页面的时间
}
//存储数据格式
type storageBlock struct {
	counterType string   //统计的类型
	storageModel string //存储的格式
	unode urlNode
}

var log = logrus.New()
//var redisCli redis.Client
func init() {
	log.Out = os.Stdout
	log.SetLevel( logrus.DebugLevel )
	// 在并发场景下需要使用RedisPool
	//redisCli, err := redis.Dial("tcp", REDIS_URL )
	//if err != nil {
	//	log.Fatalln("Redis connect faile")
	//} else {
	//	defer redisCli.Close()
	//}
}

var closeChannel chan byte

func main () {
	// 获取参数
	logFilePath := flag.String("logFilePath", "./access.log","logFilePath ...")
	routineNum := flag.Int("routineNum", 500, "routineNum ...")
	l := flag.String("l", "./log_analysis", "log file path...")
	flag.Parse()
	params := cmdParams {
		*logFilePath, *routineNum,
	}
	//打印日志
	logFd, err := os.OpenFile( *l, os.O_CREATE|os.O_WRONLY, 0644 )
	defer logFd.Close()
	if err == nil {
		//使用日志文件来存日志
		log.Out = logFd
	} else {
		log.Infof("open logFilePath err:%s", err.Error())
	}
	log.Infoln("Exec start.")
	log.Infof("Params: logFilePath=%s, routineNum=%d", params.logFilePath, params.routineNum)

	// 初始化一些 channel 用于数据传递
	routine_num := params.routineNum
	var logChannel = make(chan string, 3 * routine_num )
	var pvChannel = make(chan urlData, routine_num )
	var uvChannel = make(chan urlData, routine_num )
	var storageChannel = make(chan storageBlock, routine_num )
	closeChannel = make(chan byte)
	// Redis poll 并制定连接池的大小
	redisPool , err := pool.New("tcp", REDIS_URL, 2 * params.routineNum)
	if err != nil {
		log.Fatalln("Redis pool created failed.")
		// redis连接创建失败是重大错误
		panic(err)
	} else {
		// 当半夜没有日志时候 连接池会空闲，这时redis会断开连接
		go func() {
			//开启一个goroutine 给redis连接池发送心跳 保证连接不断开
			// 保证连接不断开 在高并发场景下至关重要
			for {
				redisPool.Cmd( "PING" )
				time.Sleep( 3 * time.Second )
			}
		}()
	}

	// 日志消费者，日志消费不适合在多个goroutine 避免重复读取
	go readFileLinebyLine(  params, logChannel )

	//创建一组日志处理
	for i:=0 ; i<params.routineNum; i++ {
		go logConsumer( logChannel, pvChannel, uvChannel )
	}

	//创建PV UV 统计器
	go pvCounter( pvChannel, storageChannel )
	go uvCounter( uvChannel, storageChannel , redisPool)
	// 这里可以声明很多个处理器，方便扩展

	//创建存储器
	go dataStorage( storageChannel , redisPool)

	//等待....
	//time.Sleep(1000 * time.Second)
	<- closeChannel
	fmt.Println("Server done.")
}

func dataStorage( storageChannel chan storageBlock, redisPool *pool.Pool ) {
	for block := range storageChannel {
		prefix := block.counterType + "_"
		//
		// 线上统计
		// 逐层加洋葱皮 也就是 访问子页面时父级要加1
		// 维度 天  小时  分钟
		// 层级 --大分类--小分类--终极页面
		// 存储模型 Redis SortedSet 有序集合
		setKeys := []string {
			//时间量级也要加洋葱皮
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" +getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" +getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" +getTime(block.unode.unTime, "min"),
		}
		//有序集合还需要成员名
		rowId := block.unode.unRid  //资源ID
		for _ ,key := range setKeys {
			ret , err := redisPool.Cmd( block.storageModel, key, 1, rowId).Int()
			// 如果没有成功 或者报错
			if ret <= 0 || err != nil {
				log.Errorln("dataStorage redis srorage error.", block.storageModel, key, rowId)
				// 个别错误 忽略 如果错误的量级比较多时  才做相应处理
			} else {
				// TODO PASS
			}
		}
	}
	close(closeChannel)
}

func pvCounter( pvChannel chan urlData, storagecChannel chan storageBlock ) {
	//一天访问网站有多少次数
	for data := range pvChannel {
		sItem := storageBlock {
			counterType:"pv",
			storageModel:"ZINCRBY",
			unode:data.unode,
		}
		storagecChannel <- sItem
	}
}

func uvCounter( uvChannel chan urlData, storagecChannel chan storageBlock, redisPool *pool.Pool) {
	// uv 需要去重
	// 一天访问网站有多少人  pv >= uv
	for data := range uvChannel {
		// HyperLoglog redis 检查去重 (去重是在一定范围内去重的，比如按天)
		hyperLogLogKey := "uv_hpll_" + getTime( data.data.time, "day")
		ret , err := redisPool.Cmd( "PFADD", hyperLogLogKey, data.uid, "EX", 86400).Int()
		if err != nil {
			log.Warningf("uvCounter check redis hyperLogLogKey fail:%s", err.Error())
			//如果查询失败 就当是没有
		}
		// 不等于1 说明已经有了
		if ret != 1 {
			continue
		}
		sItem := storageBlock{
			counterType:"uv",
			storageModel:"ZINCRBY",
			unode:data.unode,
		}
		// pv 要再uv基础上进行去重
		storagecChannel <- sItem
	}
}

func logConsumer(logChannel chan string, pvChannel , uvChannel chan urlData )  error {
	for logStr := range logChannel {
		// 切割日志, 扣出打点上报日志的数据
		data := cutLogFetchData( logStr )
		//uid
		// 说明 生成uid md5(refer+ua)
		hasher := md5.New()
		hasher.Write([]byte( data.refer + data.ua ) )
		uid := hex.EncodeToString( hasher.Sum(nil) )
		// 真实环境的urlData 会很复杂，会包含很多信息
		uData := urlData {
			data,uid, formatUrl( data.url, data.time),
		}
		pvChannel <- uData
		uvChannel <- uData
	}
	return nil
}

func cutLogFetchData( logStr string) digData {
	logStr = strings.TrimSpace( logStr )
	pos1 := str.IndexOf( logStr, HANDLE_DIG, 0)
	// 日志格式不正确
	if pos1 == -1 {
		return digData{}
	}
	pos1 += len( HANDLE_DIG )
	pos2 := str.IndexOf( logStr, " HTTP/", pos1)
	d := str.Substr( logStr, pos1, pos2-pos1 )  //截取字符串
	urlInfo , err := url.Parse("http://localhost/?" + d)  //要拼接一个完整的网址
	if err != nil {
		return digData{}
	}
	data := urlInfo.Query() //返回一个K-V的map结构
	//data 里面存储格式是K-V
	return digData {
		data.Get("time"),
		data.Get("refer"),
		data.Get("url"),
		data.Get("ua"),
	}
}

func readFileLinebyLine( params cmdParams, logChannel chan string) error {
	fd , err := os.Open( params.logFilePath)
	if err != nil {
		log.Warningf("readFileLinebyLine cant open file:%s", params.logFilePath)
		return err
	}
	defer fd.Close()
	count := 0
	bufferRead := bufio.NewReader( fd )
	for {
		//逐行读取日志 ，直到读取出\n就发送,也就是读了一行
		line , err := bufferRead.ReadString('\n')
		//log.Infof("line:%s", line)
		logChannel <- line
		count++
		if count % (1000*params.routineNum) == 0 {
			log.Infof("readFileLinebyLine line:%d", count)
		}
		if err != nil {
			//文件已经消费完了，比如半夜 文件没有新增
			if err == io.EOF {
				// 针对线上环境进行优化
				time.Sleep( 3 * time.Second )
				log.Infof("readFileLinebyLine wait,readLine:%d", count)
			} else {
				// 这里是否退出程序或者宕机 取决于错误信息
				log.Warningf("readFileLinebyLine read log err:%s", err.Error())
			}
		}
	}
	return nil
}

func formatUrl(url ,t string) urlNode {
	// 一定从量大的着手,  详情页 > 列表页 >= 首页
	// 先处理详情页 /movie是详情页面
	pos1 := str.IndexOf( url , HANDLE_MOVIE, 0)
	if pos1 != -1 {
		pos1 += len( HANDLE_MOVIE )
		pos2 := str.IndexOf( url, HANDLE_HTML, 0)
		idStr := str.Substr( url, pos1, pos2-pos1 )  //截取详情页面ID
		id , _ := strconv.Atoi( idStr )
		return urlNode {
			"movie",
			id,
			url,
			t,
		}
	} else {
		pos1  = str.IndexOf( url , HANDLE_LIST, 0 )
		if pos1 != -1 {
			pos1 += len( HANDLE_LIST )
			pos2 := str.IndexOf( url, HANDLE_HTML, 0 )
			idStr := str.Substr( url, pos1, pos2-pos1 )  //截取列表页ID
			id ,_ := strconv.Atoi( idStr )
			return urlNode{
				"list",
				id,
				url,
				t,
			}
		} else {
			//首页
			return urlNode{
				"home",
				1, //为0时redis不能写入
				url,
				t,
			} //这里可以拓展别的页面
		}
	}
}

func getTime( logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
		break
	case "hour":
		item = "2006-01-02 15"
		break
	case "min":
		item = "2006-01-02 15:09"
		break
	}
	t, _ := time.Parse( item, time.Now().Format(item))
	// 把64位Unix时间戳 转成10进制字符串
	return strconv.FormatInt( t.Unix() , 10)
}



