package main

import (
	"flag"
	"fmt"
	"strings"
	"strconv"
	"net/url"
	"math/rand"
	"time"
	"os"
)

type resource struct {
	url		string
	target	string
	start 	int
	end 	int
}

var uaList = []string {
	"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:46.0) Gecko/20100101 Firefox/46.0",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36 OPR/37.0.2178.32",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586",
	"Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
	"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)",
	"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0)",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.9.2.1000 Chrome/39.0.2146.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 Core/1.47.277.400 QQBrowser/9.4.7658.400",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Mobile/11D257 QQ/5.2.1.302 NetType/WIFI Mem/28",
	"Mozilla/5.0 (Linux; Android 5.0; SM-N9100 Build/LRX21V) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile Safari/537.36 MicroMessenger/6.0.2.56_r958800.520 NetType/WIFI",
}

func ruleResource () []resource {
	var res []resource
	r1 := resource{
		url: "http://localhost:8888/",
		target: "",
		start:0,
		end: 0,
	}
	r2 := resource{
		url: "http://localhost:8888/list/{$id}.html",
		target: "{$id}",
		start: 1,
		end: 21,
	}
	r3 := resource{
		url: "http://localhost:8888/movie/{$id}.html",
		target: "{$id}",
		start:1,
		end:12924,
	}
	res = append(append(append(res, r1), r2), r3)
	return res
}

func buildUrl (res []resource) []string {
	var list []string
	for  _, resItem := range res {
		if len(resItem.target) == 0 {
			list = append(list, resItem.url)
		} else {
			for i:= resItem.start; i <=resItem.end; i++ {
				urlStr := strings.Replace( resItem.url, resItem.target, strconv.Itoa(i) ,-1 )
				list = append(list, urlStr)
			}
		}
	}
	return list
}

func makeLog(current, refer, ua string)  string {
	u := url.Values{}
	time := time.Now().String()
	u.Set("time", time)
	u.Set("url", current)
	u.Set("refer", refer)
	u.Set("ua", ua)
	paramStr := u.Encode()
	logTemplate := "127.0.0.1 - - [08/Mar/2018:00:48:34 +0800] \"OPTIONS /dig?{$paramStr} HTTP/1.1\" 200 43 \"_\" \"{$ua}\" \"_\""
	log := strings.Replace( logTemplate, "{$paramStr}", paramStr, -1)
	log = strings.Replace( log, "{$ua}", ua, -1)
	return log
}

func randInt ( min ,max int) int {
	// 为随机数生成器提供一个种子值，保证随机值不重复
	r := rand.New(rand.NewSource( time.Now().UnixNano()))
	if min > max {
		return max
	}
	return r.Intn(max-min) + min
}

func main () {
	total := flag.Int( "total", 1000, "how mangy rows log")
	filePath := flag.String("filePath", "./access.log", "log file path")
	flag.Parse()
	// 需要构造出真实的网站URL集合
	res := ruleResource()
	list := buildUrl(res)
	//按照要求 生成$total 行日志内容  来源于上面这个集合
	fd , _ := os.OpenFile( *filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	defer fd.Close()
	startTime := time.Now()
	for i :=0 ; i< *total; i++ {
		logStr := ""
		currentUrl := list[ randInt( 0, len(list)-1)]
		referUrl := list[randInt( 0, len(list)-1)]
		ua := uaList[randInt( 0, len(uaList)-1)]
		logStr = makeLog( currentUrl, referUrl, ua) + "\n"
		fd.Write([]byte(logStr))
		fmt.Printf("Write sucess line:%d\n", i+1)
	}
	endTime := time.Now()
	fmt.Println("consum time:", endTime.Sub(startTime))
	fmt.Println("done .\n")
}


