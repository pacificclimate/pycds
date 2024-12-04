files=$(cat "$1")
for f in $files
do
  echo "file: $f"
  cat < "$f" > "${f}"_copy
done
