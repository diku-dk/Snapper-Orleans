$realScaleOut = $false

if ($realScaleOut)
{
    $workerID = 0
    Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess $workerID"
}
else
{
    $numSilo = 2
    # run worker
    for ($workerID = 0; $workerID -le $numSilo - 1; $workerID++)
    {
        Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess $workerID"
        Start-Sleep 5
    }
}