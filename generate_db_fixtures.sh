set -ex

for gtfs in tests/tasks/fixtures/*.zip
do
    python3 generate_db_from_gtfs.py -o "tests/tasks/fixtures/$(basename "$gtfs" .zip).db" "$gtfs"
done
