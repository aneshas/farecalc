# farecalc
Fare Estimation script

# Usage
The script can accept two parameters: `farecalc paths.csv fares.csv` (input csv and output csv). If you omit any of those, 
the script will default to `stdin` or `stdout` respectively, which means you can also pipe data through it, eg:
`cat paths.csv | farecalc >> out.csv`

# Design Overview
The main requirements for this script were: use of concurrency, be performant and the fact that it should be "a script", not an application.
Thus, I chose to go with a very simple design for this script (could be even simpler), eg. no sub-packages, I was sparse with types, interfaces,
decoupling, no use of decimal's etc...

The script basically has 3 components:
- csv source / ingester - reads csv lines, groups them by rideID and dispatches those groups of paths as work.
- pool of workers which consume the work (paths), parse segments and filter them, segment fare calculation and aggregation. (separation of concerns
could be a lot better here, but as I said, due to performance requirements I did not want to do any more work or allocations than neccessary)
- an output sink which consumes fare estimations and writes them to output.

I also included some unit tests and a single integration test (golden file). (I was not exhaustive here)
