[![Build Status](https://travis-ci.org/gfleury/gstreamtop.svg?branch=master)](https://travis-ci.org/gfleury/gstreamtop) [![codecov](https://codecov.io/gh/gfleury/gstreamtop/branch/master/graph/badge.svg)](https://codecov.io/gh/gfleury/gstreamtop)

# gstreamtop

Gstreamtop is a text stream SQL query tool (fancy wording). It basically maps plain text lines/json/csv entries as SQL table fields and allowing to run SQL queries. The main purpose is aggregation (queries must have GROUP BY).
Simplifying you can tail a log file and run a SQL query aggregating for something. Differently from ELK stack or Kafka + KSQL, the idea is to have something locally that you can run quickly (as a test or probe) and without external dependencies.

## Example

[![asciicast](https://asciinema.org/a/Y8qSzmLxPFXFETAMCbCtcYWdB.png?autoplay=1)](https://asciinema.org/a/Y8qSzmLxPFXFETAMCbCtcYWdB?autoplay=1)

## Testing

```bash
gstreamtop$ make
gstreamtop$ tail -f /var/log/nginx/access_log | ./gstreamtop runQuery combinedlog "SELECT URLIFY(url) as url, COUN(*) as count, SUM(size) as sum, size, MAX(response) FROM log GROUP BY url, size ORDER BY count ASC, size DESC LIMIT 20;"
```

## Mappings

The text to field maps is done on the mappings.yaml file. The regex in 'FIELDS IDENTIFIED BY' is the one that creates the mapping.

```yaml
- name: combinedlog
  tables:
  - CREATE TABLE log(ip VARCHAR, col2 VARCHAR, col3 VARCHAR, dt VARCHAR, method VARCHAR,
    url VARCHAR, version VARCHAR, response INTEGER, size INTEGER, referer VARCHAR, useragent
    VARCHAR) WITH FIELDS IDENTIFIED BY '^(?P<ip>\\S+)\\s(?P<col2>\\S+)\\s(?P<col3>\\S+)\\s\\[(?P<dt>[\\w:\\/]+\\s[+\\-]\\d{4})\\]\\s"(?P<method>\\S+)\\s?(?P<url>\\S+)?\\s?(?P<version>\\S+)?"\\s(?P<response>\\d{3}|-)\\s(?P<size>\\d+|-)\\s?"?(?P<referer>[^"]*)"?\\s?"?(?P<useragent>[^"]*)?"?$'
    LINES TERMINATED BY '\n';
```

## Prometheus Exporter

You can export queries as Prometheus metrics and visualize them on a grafana. (This was not the original idea of the tool but it happened to be good to have at the end).
