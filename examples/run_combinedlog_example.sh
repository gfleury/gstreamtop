while true; do curl "https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs" 2> /dev/null; sleep 1; done | ./gstreamtop runNamedQuery combinedlog responsebyurl

