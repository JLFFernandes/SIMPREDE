import feedparser

feed_url = "https://news.google.com/rss/search?q=cheias+Lisboa+Portugal&hl=pt-PT&gl=PT&ceid=PT:pt"
feed = feedparser.parse(feed_url)

for entry in feed.entries:
    print("🔗", entry.link)
    print("📰", entry.title)
    print("📅", entry.published)
    print("---")
