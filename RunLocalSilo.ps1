$realScaleOut = $false

if ($realScaleOut)
{
    # run this local silo
    $isGlobalSilo = $false
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $isGlobalSilo"
}
else
{
    # run all local silo
    $siloID = 0
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
    Start-Sleep 60

    $numSilo = 2
    for ($siloID = 1; $siloID -le $numSilo - 1; $siloID++)
    {
        Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $siloID"
        Start-Sleep 5
    }
}