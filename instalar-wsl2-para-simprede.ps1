<#
.SINOPSE
    Automatiza a configuração do WSL2 para executar o projeto SIMPREDE
.DESCRIÇÃO
    Este script instala o WSL2 com Ubuntu, configura o Docker Desktop,
    e prepara o ambiente para o projeto SIMPREDE
.NOTAS
    Execute este script como Administrador no PowerShell
#>

# Verificar se está executando como administrador
if (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Warning "Por favor, execute como Administrador!"
    exit
}

Write-Host "🚀 Configurando o ambiente WSL2 para o SIMPREDE..." -ForegroundColor Cyan

# Passo 1: Verificar compatibilidade da versão do Windows
$osInfo = Get-WmiObject -Class Win32_OperatingSystem
$buildNumber = [int]($osInfo.BuildNumber)
if ($buildNumber -lt 18362) {
    Write-Host "❌ Sua versão do Windows não atende aos requisitos do WSL2." -ForegroundColor Red
    Write-Host "WSL2 requer Windows 10 versão 1903 ou superior com Build 18362 ou superior." -ForegroundColor Red
    exit
}

# Passo 2: Ativar recursos necessários do Windows
Write-Host "📦 Ativando recursos necessários do Windows..." -ForegroundColor Yellow
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

# Passo 3: Baixar e instalar a atualização do kernel do WSL2
Write-Host "📥 Baixando a atualização do kernel do WSL2..." -ForegroundColor Yellow
$wslUpdateUrl = "https://wslstorestorage.blob.core.windows.net/wslblob/wsl_update_x64.msi"
$wslUpdateInstallerPath = "$env:TEMP\wsl_update_x64.msi"
Invoke-WebRequest -Uri $wslUpdateUrl -OutFile $wslUpdateInstallerPath -UseBasicParsing
Write-Host "🔧 Instalando a atualização do kernel do WSL2..." -ForegroundColor Yellow
Start-Process -FilePath "msiexec.exe" -ArgumentList "/i `"$wslUpdateInstallerPath`" /quiet" -Wait

# Passo 4: Definir WSL2 como padrão
Write-Host "⚙️ Configurando WSL2 como padrão..." -ForegroundColor Yellow
wsl --set-default-version 2

# Passo 5: Instalar Ubuntu
Write-Host "🐧 Instalando o Ubuntu..." -ForegroundColor Yellow
# Primeiro tenta usar o comando da Windows Store
try {
    Invoke-Expression "winget install Canonical.Ubuntu"
} catch {
    # Método alternativo - usa download direto
    Write-Host "⚠️ Falha na instalação pelo winget, tentando método alternativo..." -ForegroundColor Yellow
    $ubuntuAppx = "$env:TEMP\ubuntu.appx"
    Invoke-WebRequest -Uri "https://aka.ms/wslubuntu2004" -OutFile $ubuntuAppx -UseBasicParsing
    Add-AppxPackage -Path $ubuntuAppx
}

# Passo 6: Baixar o instalador do Docker Desktop
Write-Host "🐳 Configurando Docker Desktop com integração WSL2..." -ForegroundColor Yellow
$dockerInstallerPath = "$env:TEMP\DockerDesktopInstaller.exe"
Invoke-WebRequest -Uri "https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe" -OutFile $dockerInstallerPath -UseBasicParsing

# Passo 7: Instalar Docker Desktop com backend WSL2
Write-Host "🔧 Instalando Docker Desktop (isso pode demorar um pouco)..." -ForegroundColor Yellow
Start-Process -FilePath $dockerInstallerPath -ArgumentList "install --quiet --accept-license --wsl-default-version=2" -Wait

# Passo 8: Criar script de automação para configuração do Ubuntu
$setupScript = @'
#!/bin/bash
# Script de configuração para SIMPREDE no Ubuntu WSL2

# Atualizar e instalar dependências
echo "📦 Atualizando o Ubuntu e instalando dependências..."
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y \
    git \
    python3-pip \
    python3-venv \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev

# Instalar ferramentas CLI do Docker
echo "🐳 Instalando ferramentas CLI do Docker..."
sudo apt-get install -y docker-compose

# Criar diretório de trabalho
echo "📂 Criando diretório de trabalho..."
mkdir -p ~/projects

# Clonar repositório SIMPREDE se solicitado
read -p "❓ Deseja clonar o repositório SIMPREDE? (s/n): " clone_repo
if [[ $clone_repo == "s" || $clone_repo == "S" ]]; then
    echo "🔄 Clonando repositório SIMPREDE..."
    git clone https://github.com/seu-usuario/SIMPREDE.git ~/projects/SIMPREDE
    
    # Criar ambiente virtual Python
    echo "🐍 Configurando ambiente virtual Python..."
    cd ~/projects/SIMPREDE
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    
    echo "✅ SIMPREDE foi clonado e configurado com sucesso!"
else
    echo "⏭️ Pulando a clonagem do repositório."
fi

echo "🎉 Configuração do ambiente WSL2 concluída!"
echo "👉 Para começar a usar o WSL2: abra uma nova janela do PowerShell e digite 'wsl' ou inicie o Ubuntu pelo menu Iniciar"
echo "👉 Seu drive C: do Windows está acessível no WSL em /mnt/c"
echo "👉 O Docker Desktop deve estar funcionando com integração WSL2"
'@

# Salvar o script de configuração
$setupScriptPath = "$env:TEMP\simprede_wsl_setup.sh"
$setupScript | Out-File -FilePath $setupScriptPath -Encoding ASCII

Write-Host "⏳ Aguardando a instalação completa do Ubuntu... (10 segundos)" -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Passo 9: Copiar o script de configuração para o diretório home do Ubuntu e executar a configuração inicial
Write-Host "🔄 Inicializando o Ubuntu. Você precisará criar um nome de usuário e senha." -ForegroundColor Yellow
Start-Process "ubuntu" -ArgumentList "install --root" -Wait

Write-Host "📝 Copiando script de configuração para o Ubuntu..." -ForegroundColor Yellow
wsl -d Ubuntu -e bash -c "cat > ~/simprede_wsl_setup.sh" < $setupScriptPath
wsl -d Ubuntu -e bash -c "chmod +x ~/simprede_wsl_setup.sh"

Write-Host @"

🎉 Instalação do WSL2 concluída!

Próximos passos:
1. Reinicie o computador para garantir que todas as alterações entrem em vigor
2. Inicie o Ubuntu pelo menu Iniciar
3. Execute o script de configuração com: ~/simprede_wsl_setup.sh
4. Inicie o Docker Desktop e verifique se a integração WSL2 está ativada nas Configurações

Após a configuração ser concluída, você poderá executar o SIMPREDE dentro do ambiente Ubuntu WSL2.
"@ -ForegroundColor Green