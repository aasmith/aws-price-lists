require "net/http"
require "uri"

require "json"

OFFER_INDEX_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json"
OFFER_INDEX_URI = URI.parse(OFFER_INDEX_URL).freeze

def fetch(url)
  warn "Fetching %s... " % url

  uri = URI === url ? url : URI.parse(url)

  response = Net::HTTP.get_response(uri)

  unless Net::HTTPSuccess === response
    abort "Response code was not successful: %s" % [response.code]
  end

  JSON.parse response.body
end

def build_url(path, csv: false)
  base_uri = OFFER_INDEX_URI.dup
  base_uri.path = csv ? path.sub(/\.json$/, ".csv") : path
  base_uri
end

def curl_cmd(url, code)
  fname = 'prices/%s.csv' % [code].join("__")

  %q[curl -q --retry 5 --compressed -w 'Fetched (%%{http_code}) %%{filename_effective} in %%{time_total}s\n' --create-dirs -sSf -o "%s" -z "%s" "%s"] % [
    fname, fname, url
  ]
end

warn "Getting offer codes & global price lists:"

offer_urls = {}
json = fetch OFFER_INDEX_URL

t = []

json["offers"].each do |_, offer|
  oc = offer["offerCode"]

  offer_urls[oc] = build_url(offer["currentRegionIndexUrl"])

  t << Thread.new do
    system curl_cmd(build_url(offer["currentVersionUrl"], csv: true), oc)
  end

  break
end

t.each(&:join)
