generate_db_bench() {
    local N="${1:-1e7}";
    local K="${2:-1e2-0}";
    local NAs="${3:-0}";
    local sort="${4:-0}";

    frawk \
        -B cranelift \
        -v "N=${N}" \
        -v "K=${K}" \
        -v "NAs=${NAs}" \
        -v "sort=${sort}" \
        '
        function rand_int(x) {
            return 1 + int(rand() * x);
        }

        BEGIN {
           # Convert input variables to numbers (needed in case they are in scientific notation).
           N = int(N + 0);
           K = int(K + 0);
           NAs = int(NAs + 0);

           # Set fixed seed for random number generator.
           srand(123);

           # Print header.
           print "id1,id2,id3,id4,id5,id6,v1,v2,v3";

           if (sort != 1) {
               for (i=0; i<N; i++) {
                   printf("id%03d,id%03d,id%010d,%d,%d,%d,%d,%d,%.06f\n", rand_int(K), rand_int(K), rand_int(N/K), rand_int(K), rand_int(K), rand_int(N/K), rand_int(5), rand_int(15), rand() * 100);
               }
           } else {
               for (i=0; i<N; i++) {
                   printf("id%03d,id%03d,id%010d,%d,%d,%d,%d,%d,%.06f\n", rand_int(K), rand_int(K), rand_int(N/K), rand_int(K), rand_int(K), rand_int(N/K), rand_int(5), rand_int(15), rand() * 100) | "LC_COLLATE=C sort --parallel=2 -k 1,1 -k2,2 -k3,3 -k4,4n -k 5,5n -k 6,6n";
               }
           }
        }
        '
}
