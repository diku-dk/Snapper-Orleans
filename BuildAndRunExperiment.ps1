# First build the Orleans vNext nuget packages locally
if((Test-Path "..\..\vNext\Binaries\Debug\") -eq $false) {
     # this will only work in Windows.
     # Alternatively build the nuget packages and place them in the <root>/vNext/Binaries/Debug folder
     # (or make sure there is a package source available with the Orleans 2.0 TP nugets)
    #..\..\Build.cmd netstandard
}

# Uncomment the following to clear the nuget cache if rebuilding the packages doesn't seem to take effect.
#dotnet nuget locals all --clear

dotnet restore
if ($LastExitCode -ne 0) { return; }

dotnet build --no-restore
if ($LastExitCode -ne 0) { return; }

$zipF = 0.5, 1.0, 1.5, 2.0
$percent = 100, 90, 75, 50, 25, 0
$detPipe = 0
$nonDetPipe = 0
$cpu = 4

for ($j = 1; $j -le 1; $j++)
{
    $zipf = $zipF[$j]
    for ($i = 5; $i -le 5; $i++)
    {
        $detPercent = $percent[$i]
        for ($pipe = 1; $pipe -le 32; $pipe *= 2)
        {
            #$detPipe = $pipe
            $nonDetPipe = $pipe
            Start-Process "dotnet" -ArgumentList "run --project ExperimentController --no-build -- $zipF $detPercent $cpu"
            Start-Sleep 15
            Start-Process "dotnet" -ArgumentList "run --project NewProcess --no-build -- $cpu $detPipe $nonDetPipe"
            Start-Sleep 85
        }
    }
}
