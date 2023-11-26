set -ex

for fixture_name in wkd wkd-next
do
    gtfs_path="tests/tasks/fixtures/$fixture_name.zip"
    db_path="tests/tasks/fixtures/$fixture_name.db"
    python3 generate_db_from_gtfs.py -o "$db_path" "$gtfs_path"
done
