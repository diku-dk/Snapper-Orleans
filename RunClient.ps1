param(
[string] $p0 = $Args[0],
[string] $p1 = $Args[1],
[string] $p2 = $Args[2],
[string] $p3 = $Args[3]
)
Start-Process "dotnet" -ArgumentList "run --project OrleansClient --no-build -- $p0 $p1 $p2 $p3"
Start-Sleep 5