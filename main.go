package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/html"
)

// MongoDB bağlantı bilgileri
const mongoURI = "mongodb://localhost:27017"
const dbName = "dailypal"
const collectionFeeds = "rss_feeds"
const collectionNews = "news"
const workerCount = 5

type RSS struct {
	Channel Channel `xml:"channel"`
}

type Channel struct {
	Title          string   `xml:"title"`
	Link           string   `xml:"link"`
	Description    string   `xml:"description"`
	Language       string   `xml:"language"`
	ManagingEditor string   `xml:"managingEditor"`
	Category       string   `xml:"category"`
	Image          RSSImage `xml:"image"`
	Items          []Item   `xml:"item"`
}

type RSSImage struct {
	Title string `xml:"title"`
	Link  string `xml:"link"`
	URL   string `xml:"url"`
}

type MediaContent struct {
	URL        string `xml:"url,attr"`
	Type       string `xml:"type,attr"`
	Width      int    `xml:"width,attr"`
	Height     int    `xml:"height,attr"`
	Expression string `xml:"expression,attr"`
	URL2       string `xml:",chardata"`
}

type MediaThumbnail struct {
	URL    string `xml:"url,attr"`
	Width  int    `xml:"width,attr"`
	Height int    `xml:"height,attr"`
}

type Item struct {
	GUID              *GuidIsPermaLink `xml:"guid"`
	Link              string           `xml:"link"`
	Title             string           `xml:"title"`
	Description       string           `xml:"description"`
	PubDate           string           `xml:"pubDate"`
	AtomLink          string           `xml:"http://www.w3.org/2005/Atom link,attr"`
	Image             string           `xml:"image"`
	MediaContent      *MediaContent    `xml:"media:content"`
	MediaThumbnail    *MediaThumbnail  `xml:"media:thumbnail"`
	Enclosure         *Enclosure       `xml:"enclosure"`
	IpImage           string           `xml:"ipimage"`
	Creator           string           `xml:"http://purl.org/dc/elements/1.1/ creator"`
	MediaContentfor   *MediaContent    `xml:"http://search.yahoo.com/mrss/ content"`
	MediaThumbnailfor *MediaThumbnail  `xml:"http://search.yahoo.com/mrss/ thumbnail"`
	FigureImage       string           `xml:"figure>img"`
	ContentEncoded    string           `xml:"content:encoded"`
	AtomLinkforCNN    *AtomLinkforCnn  `xml:"atom:link"`
}

type AtomLinkforCnn struct {
	Href string `xml:"href,attr"`
}

type Enclosure struct {
	URL    string `xml:"url,attr"`
	Type   string `xml:"type,attr"`
	Length int    `xml:"length,attr"`
}

type GuidIsPermaLink struct {
	URL     string `xml:",chardata"`
	IsPerma bool   `xml:"isPermaLink,attr"`
}

type News struct {
	Title         string    `bson:"title"`
	Link          string    `bson:"link"`
	Description   string    `bson:"description"`
	PubDate       time.Time `bson:"pub_date"`
	Category      []string  `bson:"category"`
	Source        string    `bson:"source"`
	Creator       string    `bson:"creator"`
	Language      string    `bson:"language"`
	LastBuildDate time.Time `bson:"last_build_date"`
	ImageUrl      string    `bson:"imageUrl"`
	SubCategory   string    `bson:"sub_category"`
	Hash          string    `bson:"generatedHash"`
}

var client *mongo.Client

type RSSFeeds map[string]map[string][]string

func initialize() {
	clientOptions := options.Client().ApplyURI(mongoURI)
	var err error
	client, err = mongo.Connect(context.TODO(), clientOptions) // Global client değişkenini ayarla
	if err != nil {
		log.Fatal(err)
	}
}
func main() {
	initialize() // MongoDB bağlantısını başlat

	router := gin.Default()

	router.POST("/import-feeds", func(c *gin.Context) {
		importFeedsHandler(c, client)
	})

	router.GET("/rssfetch", func(c *gin.Context) {
		handlerfetchrss()
	})

	fmt.Println("Server çalışıyor: http://localhost:8080")
	router.Run(":8080")
}

func handlerfetchrss() {

	feedCollection := client.Database(dbName).Collection(collectionFeeds)
	newsCollection := client.Database(dbName).Collection(collectionNews)

	cursor, err := feedCollection.Find(context.TODO(), bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())

	var wg sync.WaitGroup
	jobs := make(chan struct {
		url      string
		category string
	}, 100)

	// Worker'ları başlat (Bu kısım sadece **1 kere** çağrılmalı)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(jobs, &wg, newsCollection)
	}

	// RSS URL'lerini işle ve `jobs` kanalına ekle
	for cursor.Next(context.TODO()) {
		var rssFeed struct {
			Category string              `bson:"category"`
			Topics   map[string][]string `bson:"topics"`
		}
		err := cursor.Decode(&rssFeed)
		if err != nil {
			log.Fatal(err)
		}

		for _, urls := range rssFeed.Topics {
			for _, url := range urls {
				fmt.Println("RSS Okunuyor:", url)
				jobs <- struct {
					url      string
					category string
				}{url, rssFeed.Category}
			}
		}
	}

	// Kanalı kapat ve worker'ların bitmesini bekle
	close(jobs)
	wg.Wait()
	fmt.Println("RSS işlemi tamamlandı!")
}

// ✅ Worker fonksiyonu
func worker(jobs <-chan struct {
	url      string
	category string
}, wg *sync.WaitGroup, newsCollection *mongo.Collection) {
	defer wg.Done()

	for job := range jobs {
		channel, newsItems := fetchRSS(job.url, job.category)

		for _, news := range newsItems {
			news.Category = append(news.Category, job.category) // ✅ Kategori Doğru Ekleniyor
			news.Source = job.url
			if channel.Language == "" {
				news.Language = extractLanguageFromURL(news.Source)
			} else {
				news.Language = channel.Language
			}

			filter := bson.M{"hash": news.Hash}
			update := bson.M{"$setOnInsert": news}
			opts := options.Update().SetUpsert(true)

			result, err := newsCollection.UpdateOne(context.TODO(), filter, update, opts)
			if err != nil {
				log.Println("Haber kaydedilemedi:", err)
			} else if result.UpsertedCount > 0 {
				log.Println("Yeni haber eklendi:", news.Title)
			} else {
				log.Println("Haber zaten mevcut, eklenmedi:", news.Title)
			}
		}
	}
}

func fetchRSS(url, category string) (Channel, []News) {
	resp, err := http.Get(url)

	if err != nil {
		log.Println("RSS çekilemedi:", err)
		return Channel{}, nil
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("RSS okunamadı:", err)
		return Channel{}, nil
	}

	var rss RSS
	err = xml.Unmarshal(body, &rss)
	if err != nil {
		log.Println("RSS parse hatası:", err)
		return Channel{}, nil
	}

	var newsItems []News
	for _, item := range rss.Channel.Items {

		var pubDate time.Time
		var err error

		if item.PubDate == "" {
			fmt.Println("[WARNING] PubDate eksik, bugünün tarihi atanıyor:", item.Title)
			pubDate = time.Now()
		} else {
			formats := []string{
				time.RFC1123,
				time.RFC1123Z,
				time.RFC3339,
				"Mon, 2 Jan 2006",
			}

			for _, format := range formats {
				pubDate, err = time.Parse(format, item.PubDate)
				if err == nil {
					fmt.Println("[DEBUG] Kullanılan tarih formatı:", format)
					break
				}
			}

			if err != nil {
				fmt.Println("[ERROR] Geçerli bir tarih formatı bulunamadı!", item.PubDate)
				pubDate = time.Now() // Varsayılan tarih
			}
		}

		descriptionText := cleanHTML(item.Description)
		hash := GenerateHash(item.Title + item.Link)

		var linkItem string

		if isValidURL(item.Link) {
			linkItem = item.Link
		} else if item.AtomLinkforCNN != nil && isValidURL(item.AtomLinkforCNN.Href) { // 2️⃣ `<atom:link>` varsa al
			linkItem = item.AtomLinkforCNN.Href
		} else if item.GUID != nil && !item.GUID.IsPerma && isValidURL(item.GUID.URL) { // 3️⃣ Eğer GUID varsa ama perma değilse al
			linkItem = item.GUID.URL
		} else if isValidURL(item.AtomLink) {
			linkItem = item.AtomLink
		} else {
			log.Println("⚠️ Uyarı: Link eksik, haber atlanıyor:", item.Title)
			continue
		}

		tm := time.Now()

		newsImage := ""

		if item.MediaContent != nil && isValidURL(item.MediaContent.URL) {
			newsImage = item.MediaContent.URL
		}

		if newsImage == "" && item.MediaThumbnail != nil && isValidURL(item.MediaThumbnail.URL) {
			newsImage = item.MediaThumbnail.URL
		}
		if item.MediaContentfor != nil && isValidURL(item.MediaContentfor.URL) {
			newsImage = item.MediaContentfor.URL
		}

		if newsImage == "" && item.MediaThumbnailfor != nil && isValidURL(item.MediaThumbnailfor.URL) {
			newsImage = item.MediaThumbnailfor.URL
		}

		if newsImage == "" && isValidURL(item.Image) {
			newsImage = item.Image
		}

		if newsImage == "" && item.MediaContent != nil {
			fmt.Println("MediaContent URL bulundu:", item.MediaContent.URL)
			newsImage = item.MediaContent.URL
		}
		if newsImage == "" && item.MediaContent != nil {
			fmt.Println("MediaContent URL bulundu:", item.MediaContent.URL)
			newsImage = item.MediaContent.URL2
		}

		if newsImage == "" && item.FigureImage != "" {
			newsImage = item.FigureImage
		}

		if newsImage == "" && item.Description != "" {
			doc, err := html.Parse(strings.NewReader(item.Description))
			if err == nil {
				newsImage = extractImageURL(doc)
			}
		}

		if newsImage == "" && item.Enclosure != nil {
			fmt.Println("Enclosure bulundu:", item.Enclosure.URL)
			newsImage = item.Enclosure.URL
		}

		if newsImage == "" && item.IpImage != "" {
			fmt.Println("IpImage bulundu:", item.IpImage)
			newsImage = item.IpImage
		}

		newsItems = append(newsItems, News{
			Title:         item.Title,
			Link:          linkItem,
			Description:   descriptionText,
			PubDate:       pubDate,
			ImageUrl:      newsImage,
			Hash:          hash,
			SubCategory:   category,
			LastBuildDate: tm,
			Creator:       item.Creator,
		})

	}

	return rss.Channel, newsItems
}

func cleanHTML(htmlString string) string {

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlString))
	if err != nil {
		log.Println("HTML parse hatası:", err)
		return htmlString
	}

	return doc.Text()
}

func isValidURL(url string) bool {
	return strings.HasPrefix(url, "http")
}

func extractImageURL(n *html.Node) string {
	var imageURL string

	if n.Type == html.ElementNode {
		switch n.Data {
		case "img":
			for _, attr := range n.Attr {
				if attr.Key == "src" {
					imageURL = attr.Val
				}
			}
		case "media:content":
			for _, attr := range n.Attr {
				if attr.Key == "url" {
					imageURL = attr.Val
				}
			}
		case "enclosure":
			for _, attr := range n.Attr {
				if attr.Key == "url" {
					imageURL = attr.Val
				}
			}
		case "image":
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.ElementNode && c.Data == "url" && c.FirstChild != nil {
					imageURL = c.FirstChild.Data
				}
			}
		}
	}

	if n.Type == html.ElementNode && n.Data == "enclosure" {
		for _, attr := range n.Attr {
			if attr.Key == "img" {
				imageURL = attr.Val

			}
			if attr.Key == "image" {
				imageURL = attr.Val

			}
			if attr.Key == "url" {
				imageURL = attr.Val
			}
		}
	}

	if n.Type == html.ElementNode && n.Data == "media:content" {
		for _, attr := range n.Attr {
			if attr.Key == "url" {
				imageURL = attr.Val

			}
			if attr.Key == "img" {
				imageURL = attr.Val

			}
			if attr.Key == "image" {
				imageURL = attr.Val

			}
		}
	}

	if n.Type == html.ElementNode && n.Data == "enclosure" {
		for _, attr := range n.Attr {
			if attr.Key == "url" {
				return attr.Val
			}
		}
	}

	if n.Type == html.ElementNode && n.Data == "media:thumbnail" {
		for _, attr := range n.Attr {
			if attr.Key == "url" {
				imageURL = attr.Val

			}
			if attr.Key == "img" {
				imageURL = attr.Val

			}
			if attr.Key == "image" {
				imageURL = attr.Val
			}
		}
	}

	if n.Type == html.ElementNode && n.Data == "image" {
		for _, attr := range n.Attr {
			if attr.Key == "url" {
				imageURL = attr.Val
			}
			if attr.Key == "img" {
				imageURL = attr.Val
			}
			if attr.Key == "image" {
				imageURL = attr.Val
			}
		}
	}

	if n.Type == html.ElementNode && (n.Data == "description" || n.Data == "content:encoded") {
		if strings.Contains(n.FirstChild.Data, "http") {
			imageURL = extractURLFromText(n.FirstChild.Data)
		}
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if imgURL := extractImageURL(c); imgURL != "" {
			imageURL = imgURL
		}
	}

	return imageURL
}

func extractLanguageFromURL(url string) string {
	// URL'nin sonunda dil kodunu çıkar
	re := regexp.MustCompile(`\.(\w+)$`) // URL'nin sonunda bir dil kodu aranıyor
	match := re.FindStringSubmatch(url)

	if len(match) > 0 {
		// URL'deki dil kodunu döndür
		return match[1]
	}
	// Dil kodu yoksa varsayılan dil olarak 'tr' döndür
	return "tr"
}

func extractURLFromText(text string) string {
	// Burada basit bir HTTP URL bulma mekanizması kullanabiliriz
	start := strings.Index(text, "http")
	if start == -1 {
		return ""
	}
	end := strings.Index(text[start:], " ")
	if end == -1 {
		end = len(text)
	}
	return text[start : start+end]
}

func ExtractText(n *html.Node) string {
	var result string
	if n.Type == html.TextNode {
		result = n.Data
	}

	for c := n.FirstChild; c != nil; c = c.NextSibling {
		result += ExtractText(c)
	}

	return result
}

func GenerateHash(title string) string {
	hash := sha256.Sum256([]byte(title))
	return hex.EncodeToString(hash[:])
}
func importFeedsHandler(c *gin.Context, client *mongo.Client) {
	// JSON dosyasını oku
	feeds, err := readRSSFeeds("rss_feeds.json")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "RSS Feed dosyası okunamadı"})
		return
	}

	// MongoDB koleksiyonunu seç
	feedCollection := client.Database(dbName).Collection(collectionFeeds)

	// MongoDB'ye ekleme işlemi
	for category, topics := range *feeds {
		filter := bson.M{"category": category}
		update := bson.M{"$set": bson.M{"category": category, "topics": topics}}
		opts := options.Update().SetUpsert(true)

		_, err := feedCollection.UpdateOne(context.TODO(), filter, update, opts)
		if err != nil {
			log.Println("MongoDB'ye eklenirken hata:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "MongoDB'ye kaydedilemedi"})
			return
		}
	}

	// Başarı mesajı döndür
	c.JSON(http.StatusOK, gin.H{"message": "RSS Feed'ler MongoDB'ye kaydedildi"})
}

// JSON dosyasını oku
func readRSSFeeds(filename string) (*RSSFeeds, error) {
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var feeds RSSFeeds
	err = json.Unmarshal(file, &feeds)
	if err != nil {
		return nil, err
	}

	return &feeds, nil
}
