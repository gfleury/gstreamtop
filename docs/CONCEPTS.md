# CONCEPTS (source wikipedia)

## Database query VS Stream query
| Database management system (DBMS) | Data stream management system (DSMS) |
|-----------------------------------|--------------------------------------|
| Persistent data (relations) | volatile data streams |
| Random access | Sequential access |
| One-time queries | Continuous queries |
| (theoretically) unlimited secondary storage | limited main memory |
| Only the current state is relevant | Consideration of the order of the input |
| relatively low update rate | potentially extremely high update rate |
| Little or no time requirements | Real-time requirements |
| Assumes exact data | Assumes outdated/inaccurate data |
| Plannable query processing | Variable data arrival and data characteristics |

## Processing and streaming models

One of the biggest challenges for a DSMS is to handle potentially infinite data streams using a fixed amount of memory and no random access to the data. There are different approaches to limit the amount of data in one pass, which can be divided into two classes. For the one hand, there are compression techniques that try to summarize the data and for the other hand there are window techniques that try to portion the data into (finite) parts.

### Synopses

The idea behind compression techniques is to maintain only a synopsis of the data, but not all (raw) data points of the data stream. The algorithms range from selecting random data points called sampling to summarization using histograms, wavelets or sketching. One simple example of a compression is the continuous calculation of an average. Instead of memorizing each data point, the synopsis only holds the sum and the number of items. The average can be calculated by dividing the sum by the number. However, it should be mentioned that synopses cannot reflect the data accurately. Thus, a processing that is based on synopses may produce inaccurate results.

### Windows

Instead of using synopses to compress the characteristics of the whole data streams, window techniques only look on a portion of the data. This approach is motivated by the idea that only the most recent data are relevant. Therefore, a window continuously cuts out a part of the data stream, e.g. the last ten data stream elements, and only considers these elements during the processing. There are different kinds of such windows like sliding windows that are similar to FIFO lists or tumbling windows that cut out disjoint parts. Furthermore, the windows can also be differentiated into element-based windows, e.g., to consider the last ten elements, or time-based windows, e.g., to consider the last ten seconds of data. There are also different approaches to implementing windows. There are, for example, approaches that use timestamps or time intervals for system-wide windows or buffer-based windows for each single processing step. Sliding-window query processing is also suitable to being implemented in parallel processors by exploiting parallelism between different windows and/or within each window extent.

## Technical details

It should extends the type system of SQL to support streams in addition to tables. Several new operations are introduced to manipulate streams.

Selecting from a stream - A standard SELECT statement can be issued against a stream to calculate functions (using the target list) or filter out unwanted tuples (using a WHERE clause). The result will be a new stream.

Stream-Relation Join - A stream can be joined with a relation to produce a new stream. Each tuple on the stream is joined with the current value of the relation based on a predicate to produce 0 or more tuples.

Union and Merge - Two or more streams can be combined by unioning or merging them. Unioning combines tuples in strict FIFO order. Merging is more deterministic, combining streams according to a sort key.

Windowing and Aggregation - A stream can be windowed to create finite sets of tuples. For example, a window of size 5 minutes would contain all the tuples in a given 5 minute period. Window definitions can allow complex selections of messages, based on tuple field values. Once a finite batch of tuples is created, analytics such as count, average, max, etc., can be applied.

Windowing and Joining - A pair of streams can also be windowed and then joined together. Tuples within the join windows will combine to create resulting tuples if they fulfill the predicate.


## Few KSQL examples

```sql
CREATE TABLE error_counts AS
SELECT error_code, count(*)FROM monitoring_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
WHERE type = 'ERROR';
```

```sql
CREATE TABLE possible_fraud AS
SELECT card_number, count(*)
FROM authorization_attempts
WINDOW TUMBLING (SIZE 5 SECONDS)
GROUP BY card_number
HAVING count(*) > 3;
```

```sql
CREATE STREAM vip_users AS
SELECT userid, page, action 
FROM clickstream c 
LEFT JOIN users u ON c.userid = u.user_id
WHERE u.level = 'Platinum';
```

https://docs.aws.amazon.com/kinesisanalytics/latest/sqlref/analytics-sql-reference.html

Windowing formats
- TUMBLING, grouping by that window time. WINDOW TUMBLING (SIZE 1 HOUR) will groupby 1 hour
- SESSION, grouping by keys that happen in that session time. 