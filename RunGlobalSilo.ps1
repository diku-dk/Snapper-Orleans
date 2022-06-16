$realScaleOut = $false

if ($realScaleOut)
{
    # run global silo
    $isGlobalSilo = $true
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $isGlobalSilo"
}
else
{
    # run global silo
    $numSilo = 2
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $numSilo"
}