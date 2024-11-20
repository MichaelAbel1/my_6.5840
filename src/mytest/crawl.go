package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/html"
)

// Fetcher 是一个接口，表示可以获取 URL 的内容和链接
type Fetcher interface {
	Fetch(url string) (body string, urls []string, err error)
}

// RealFetcher 是一个真实的 Fetcher 实现，使用 HTTP 请求来抓取网页
type RealFetcher struct{}

// Fetch 实现真实的抓取操作
func (f RealFetcher) Fetch(url string) (string, []string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("failed to fetch %s: %s", url, resp.Status)
	}

	doc, err := html.Parse(resp.Body)
	if err != nil {
		return "", nil, err
	}

	var body strings.Builder
	var urls []string

	var fu func(*html.Node)
	fu = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					urls = append(urls, a.Val)
				}
			}
		}
		if n.Type == html.TextNode {
			body.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			fu(c)
		}
	}
	fu(doc)

	return body.String(), urls, nil
}

// Crawl 函数并行地抓取 URL，并且保证不重复
func Crawl(url string, depth int, fetcher Fetcher, wg *sync.WaitGroup, visited *sync.Map) {
	defer wg.Done()

	// 如果深度为 0，则直接返回
	if depth <= 0 {
		return
	}

	// 检查 URL 是否已经被抓取过
	if _, ok := visited.Load(url); ok {
		return
	}

	// 抓取 URL
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 标记 URL 为已抓取
	visited.Store(url, true)

	fmt.Printf("found: %s %q\n", url, body)

	// 并行抓取子 URL
	for _, u := range urls {
		wg.Add(1)
		go Crawl(u, depth-1, fetcher, wg, visited)
	}
}

func main() {
	var wg sync.WaitGroup
	visited := &sync.Map{}

	wg.Add(1)
	go Crawl("https://baidu.com/", 4, RealFetcher{}, &wg, visited)

	// 等待所有 goroutine 完成
	wg.Wait()
}
