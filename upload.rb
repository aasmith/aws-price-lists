abort "need bucket name" unless ARGV.first

require "csv"
require "fileutils"
require "stringio"
require "thread"

require "aws-sdk-s3"

PARSER_THREADS = 3
UPLOAD_THREADS = 10

TEMPLATE = DATA.read.freeze

$s3 = Aws::S3::Resource.new(region:'us-west-2')
$bucket = $s3.bucket(ARGV.first)

parser_queue = Queue.new
upload_queue = Queue.new

printer = Mutex.new

t = []

Dir.glob("prices/*.csv").each do |file|
  parser_queue << file
end

parser_queue.close

PARSER_THREADS.times do |pt|
  t << Thread.new do
    while file = parser_queue.pop

      File.open(file) do |csvio|

        # Skip the first 5 lines of non-csv meta
        metas = 5.times.map { csvio.readline }.join

        meta = CSV.new(metas).map do |name, value|
          "<dt>%s</dt><dd>%s</dd>" % [name, value]
        end.join

        csv = CSV.new(csvio, headers: true)

        csv.each do |row|

          code = row["RateCode"]

          table = row.map do |name, value|
            "<tr><td>%s</td><td>%s</td></tr>" % [name, value]
          end.join


          html = TEMPLATE % [
            code, table, meta
          ]

          upload_queue << {
            key: o = "#{code}/index.html",
            body: html
          }
        end
      end
    end

    upload_queue << :done

    warn "Parser Thread ##{pt} done"
  end
end

UPLOAD_THREADS.times do |ut|
  t << Thread.new do
    loop do
      work = upload_queue.pop

      break if work == :done

      $bucket.put_object(work)

      warn "uploaded %s" % work[:key]
    end

    warn "Uploader Thread ##{ut} done"
  end
end

t.each &:join


__END__
<!doctype html>
<html lang=en>

<head>
<link rel="stylesheet" href="/style.css">
<title>AWS Rate Codes</title>
</head>

<header>
<h1>AWS Rate Codes</h1>
<h2><code>%s</code></h2>
</header>

<table>
%s
</table>

<footer>
<dl>
%s
</dl>
<p>
<strong>
This site is not maintained by or affiliated with Amazon. The data shown is not guaranteed to be accurate or current.
</strong>
</footer>
</html>

