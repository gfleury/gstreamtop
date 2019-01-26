package output

import (
	"fmt"
	"github.com/gfleury/gstreamtop/tablestream"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusOutput struct {
	StreamOutput
	metrics []*metric
}

type metricHolder interface {
	setMetric(prometheus.Collector)
	metric() prometheus.Collector
	setViewDataIdx(int)
	viewDataIdx() int
	setView(*tablestream.View)
	view() *tablestream.View
}

type metric struct {
	metricHolder
	metricData prometheus.Collector
	vdIdx      int
	v          *tablestream.View
}

func (o *metric) metric() prometheus.Collector {
	return o.metricData
}

func (o *metric) setMetric(m prometheus.Collector) {
	o.metricData = m
}

func (o *metric) viewDataIdx() int {
	return o.vdIdx
}

func (o *metric) setViewDataIdx(vd int) {
	o.vdIdx = vd
}

func (o *metric) view() *tablestream.View {
	return o.v
}

func (o *metric) setView(v *tablestream.View) {
	o.v = v
}

func (o *PrometheusOutput) Loop() {
	pTicker := time.NewTicker(time.Second * 2)

	for _, view := range o.stream.GetViews() {
		for idx, vd := range view.ViewDatas() {
			if vd.VarType() == tablestream.VARCHAR {
				continue
			}
			metric := &metric{}
			switch vd.Modifier() {
			default:
				promMetric := promauto.NewGaugeVec(prometheus.GaugeOpts{
					Name: metricName(fmt.Sprintf("%s_%s", "FirstView", vd.Name())),
					Help: fmt.Sprintf("Metrics %s are from view %s", vd.Name(), view.Name()),
				}, []string{"row"})
				metric.setMetric(promMetric)
				metric.setView(view)
				metric.setViewDataIdx(idx)
			}
			o.metrics = append(o.metrics, metric)
		}
	}

	go PrometheusHTTP()

	for *o.InputExists() {
		<-pTicker.C
		for _, metric := range o.metrics {
			keys := metric.view().OrderedKeys()
			result := metric.view().IntViewData(metric.viewDataIdx(), keys)
			for keyIdx, value := range result {
				labels := prometheus.Labels{"row": keys[keyIdx]}
				metric.metric().(*prometheus.GaugeVec).With(labels).Set(float64(value))
			}

		}

		// Just print the normal SimpleTable.
		for _, view := range o.stream.GetViews() {
			tablestream.TableWrite(view, os.Stdout)
		}
	}

}

func (o *PrometheusOutput) Configure() error {
	o.errors = make(chan error)
	return nil
}

func (o *PrometheusOutput) Shutdown() {
}

func PrometheusHTTP() {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":9090", nil))
}

func metricName(name string) string {
	return strings.Replace(strings.Replace(name, "(", "_", -1), ")", "_", -1)
}
