Add-Type -Namespace W -Name P -MemberDefinition '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint f);'
$until = [datetime]'2026-11-04 12:00'
while ((Get-Date) -lt $until) { [W.P]::SetThreadExecutionState([uint32]"0x80000001") | Out-Null; Start-Sleep -Seconds 60 }
[W.P]::SetThreadExecutionState([uint32]"0x80000000") | Out-Null

