help:
  echo "Welcome to NimbleDB. Please refer to the README for more information on building"

format:
  clang-format --dry-run -Werror $(git ls-files | grep -E -i '\.(h|c|cc)$')
