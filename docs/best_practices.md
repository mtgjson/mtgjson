# Use Linter


# Methods
```
# Bad
def filter_request(request):

# Good
# Prefer type annotation
def filter_request(request: requests.Response) -> requests.Response:
```

# Imports
```
# Prefer imports to be at the top of file
import foo
```