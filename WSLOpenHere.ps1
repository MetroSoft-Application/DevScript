# 必要な情報を設定
$KeyName = "WSLOpenHere"
$MenuName = "WSLでここを開く"
# PowerShell 内での正しいエスケープを使用してコマンドを組み立てる
$Command = 'wsl.exe --cd "%V"'

# レジストリにコンテキストメニューのエントリを追加
$KeyPath = "Registry::HKEY_CLASSES_ROOT\Directory\shell\$KeyName"
$CommandPath = "$KeyPath\command"

# キー作成
New-Item -Path $KeyPath -Force
New-Item -Path $CommandPath -Force

# メニューテキストとコマンドを設定
Set-ItemProperty -Path $KeyPath -Name "(default)" -Value $MenuName
Set-ItemProperty -Path $CommandPath -Name "(default)" -Value $Command

# 終了メッセージ
Write-Output "WSLでここを開くオプションが追加されました。エクスプローラで右クリックして確認してください。"