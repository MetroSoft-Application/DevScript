param (
    [string]$title = "Finish",
    [string]$desc = $(if ($PSBoundParameters.ContainsKey('desc')) { $desc } else { "処理が完了しました。" + " (時刻: $(Get-Date -Format 'yyyy/hh/dd HH:mm:ss'))" })
)

# BurntToastモジュールがインストールされていなければインストール
Install-Module -Name BurntToast
# 通知を表示
New-BurntToastNotification -Text $title, $desc