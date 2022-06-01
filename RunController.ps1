
function RunExperiment 
{
    param ( [int] $experimentID, [int] $numCPU, [string] $implementation, [string] $benchmark, [string] $loggingEnabled, [int] $numOrderGrainPerDistrict)
    Start-Process "dotnet" -ArgumentList "run --project SnapperSiloHost $numCPU $implementation $loggingEnabled"
    Start-Sleep 30
    Wait-Process -Name "SnapperSiloHost"
    Start-Sleep 5
}

dotnet restore
if ($LastExitCode -ne 0) { return; }

dotnet build --no-restore
if ($LastExitCode -ne 0) { return; }

$zipF = 0.0, 0.9, 1.0, 1.25, 1.5, 2.0
$percent = 50, 90, 50, 0
$detPipe = 64
$nonDetPipeAll = 64, 64, 8, 4, 4, 4
$cpu = 4

for ($j = 0; $j -le 0; $j++)
{
    $nonDetPipe = $nonDetPipeAll[$j]
    $zipf = $zipF[$j]
    for ($i = 0; $i -le 0; $i++)
    {
        $detPercent = $percent[$i]
        for ($pipe = 1; $pipe -le 1; $pipe *= 2)
        {
            #$detPipe = $pipe
            #$nonDetPipe = $pipe
            Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentController --no-build -- $zipF $detPercent $cpu"
            Start-Sleep 8
            Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess --no-build -- $cpu $detPipe $nonDetPipe"
            #Start-Process "dotnet" -ArgumentList "run --project SnapperExperimentProcess --no-build -- $cpu $pipe"
            #Start-Sleep 62
        }
    }
}

