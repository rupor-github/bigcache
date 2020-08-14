<p align="center">
    <h1 align="center">bigcache v3</h1>
    <p align="center">
        Reluctant fork of allegro/bigcache/v2
    </p>
    <p align="center">
        <a href="https://pkg.go.dev/mod/github.com/rupor-github/bigcache/v3/?tab=packages"><img alt="GoDoc" src="https://img.shields.io/badge/godoc-reference-blue.svg" /></a>
        <a href="https://goreportcard.com/report/github.com/rupor-github/bigcache"><img alt="Go Report Card" src="https://goreportcard.com/badge/github.com/rupor-github/bigcache" /></a>
    </p>
    <hr>
</p>

# BigCache

Fast, concurrent, evicting in-memory cache written to keep big number of entries without impact on performance.
BigCache keeps entries on heap but omits GC for them. To achieve that, operations on byte slices take place,
therefore entries (de)serialization in front of the cache will be needed in most use cases.

Requires Go 1.13 or newer.

This is very reluctant fork of original BigCache v2.2.2. For any documentation, issues or discussion - please, visit original project.

Original is presently unstable (see issues there - in my production tests I cannot keep it up for more than 10 minutes before it dies with index out of range) and when working on a fix I got carried away with too many changes.
While cleaning I attempted to keep code as efficient as it was while making it somewhat easier to read and modify. 
Original code has unused functions (result of functionality move to shard.go), unnecessary flags trying to fix edge conditions (q.full? really?), too many OnRemove callbacks in attempt to keep backward compatibility, outdated logger (and overall configuration - to some degree).
Locking iterator design is a bit strange for highly concurrent code. 

All in all I got a feeling that contributors are more interested in a expanding feature set with additional callbacks rather than having readable, simple, stable and performing code base as was the case for a very long time with v1.

Until I decide if I want to make upstream PR I will keep this fork - it is presently benchmarks as good or faster as original bigcache at v2.2.2 and has some additional functionality I need (see GetWithProcesssing).

## License

BigCache is released under the Apache 2.0 license (see [LICENSE](LICENSE))
