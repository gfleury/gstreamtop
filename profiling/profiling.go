package profiling

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

func EnableCPUProfile(cpuprofile string) func() {
	f, err := os.Create(cpuprofile)
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	return pprof.StopCPUProfile
}

func DumpMemory(memprofile string) {
	f, err := os.Create(memprofile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
	f.Close()
}
