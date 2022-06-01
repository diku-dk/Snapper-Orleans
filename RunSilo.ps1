# run silo
$siloID = 0
Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
Start-Sleep 60

for ($siloID = 1; $siloID -le 8; $siloID++)
{
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
    Start-Sleep 5
}