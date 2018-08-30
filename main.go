package main // import "whosthebusyboi"

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/lib/pq"
	"golang.org/x/crypto/ssh/terminal"
)

type Relation struct {
	dbOID  string
	relOID string
}

type RelationRank struct {
	r    Relation
	rank int
}

var totalPoints = 0
var relations = make(map[Relation]int)
var relationMapping = make(map[string]string)
var relationsLock sync.RWMutex

func findCheckpointerPid() (int, error) {
	d, err := os.Open("/proc")
	if err != nil {
		return 0, err
	}
	defer d.Close()
	for {
		fis, err := d.Readdir(10)
		if err != nil {
			return 0, err
		}
		for _, fi := range fis {
			if !fi.IsDir() {
				continue
			}

			name := fi.Name()
			if name[0] < '0' || name[0] > '9' {
				continue
			}

			pid, err := strconv.ParseInt(name, 10, 0)
			if err != nil {
				continue
			}

			data, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			if err != nil {
				continue
			}

			if bytes.Equal(data[:30], []byte("postgres: checkpointer process")) {
				return int(pid), nil
			}
		}
	}
}

func handleStraceLine(b []byte) {
	if len(b) < 12 {
		return
	}
	if !bytes.Equal(b[:11], []byte(`open("base/`)) {
		return
	}
	i := 11
	dbStart := i
	dbStop := 0
	for ; i < len(b); i++ {
		if b[i] == '/' {
			dbStop = i
			i++ // skip over '/'
			break
		}
	}
	relStart := i
	relStop := 0
	for ; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			relStop = i
			break
		}
	}

	rel := Relation{
		string(b[dbStart:dbStop]),
		string(b[relStart:relStop]),
	}

	relationsLock.Lock()
	_, ok := relations[rel]
	if !ok {
		relations[rel] = 0
	}
	relations[rel]++
	relationsLock.Unlock()

	totalPoints++
}

var (
	flagUser     = flag.String("U", "postgres", "Postgres user")
	flagInterval = flag.Duration("i", time.Second, "How long between ticks?")
)

func main() {
	flag.Parse()

	dbName := flag.Arg(0)
	if dbName == "" {
		dbName = *flagUser
	}
	db, err := sql.Open("postgres", fmt.Sprintf("user=%s dbname=%s sslmode=disable", *flagUser, dbName))
	if err != nil {
		panic(err)
	}

	pid, err := findCheckpointerPid()
	if err != nil {
		panic(err)
	}
	strace := exec.Command("strace", "-p", strconv.Itoa(pid))
	stderr, err := strace.StderrPipe()
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(stderr)
	go func() {
		for {
			b, err := r.ReadBytes('\n')
			if err == io.EOF {
				log.Println(err)
				return
			}
			if err != nil {
				log.Println(err)
				return
			}
			handleStraceLine(b)
		}
	}()
	go func() {
		for range time.Tick(*flagInterval) {
			var ss []RelationRank
			relationsLock.RLock()
			for k, v := range relations {
				ss = append(ss, RelationRank{k, v})
			}
			relationsLock.RUnlock()

			sort.Slice(ss, func(i, j int) bool {
				return ss[i].rank > ss[j].rank
			})

			_, height, err := terminal.GetSize(0)
			if err != nil {
				height = 10
			}
			if len(ss) > height-3 {
				ss = ss[:height-3]
			}

			total := float64(totalPoints)

			missingRelations := make([]string, 0)
			for i := 0; i < len(ss); i++ {
				if _, ok := relationMapping[ss[i].r.relOID]; !ok {
					missingRelations = append(missingRelations, ss[i].r.relOID)
				}
			}

			if len(missingRelations) > 0 {
				rows, err := db.Query(`SELECT relfilenode, relname FROM pg_class WHERE relfilenode = ANY($1)`, pq.Array(missingRelations))
				if err != nil {
					panic(err)
				}
				for rows.Next() {
					var relfilenode string
					var relname string
					rows.Scan(&relfilenode, &relname)
					relationMapping[relfilenode] = relname
				}
				rows.Close()
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
			for _, kv := range ss {
				fmt.Fprintf(w, "%s\t%.2f%%\n", relationMapping[kv.r.relOID], (float64(kv.rank)/total)*100)
			}

			fmt.Println("\033[2J")
			fmt.Println("Total:", int(total))
			fmt.Println()
			w.Flush()
		}
	}()
	if err := strace.Start(); err != nil {
		panic(err)
	}
	log.Println(strace.Wait())
}
