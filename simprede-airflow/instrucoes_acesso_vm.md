
# Instruções para Acesso à VM GCP - Projeto SIMPREDE


---

## Requisitos

1. Conta Google com permissões no projeto (IAM Roles): **Falta O Paulo**
   - `Compute Instance Admin (v1)`
   - `Storage Object Admin`

2. Google Cloud SDK instalado:  
   https://cloud.google.com/sdk/docs/install

3. (Opcional mas recomendado):
   - VS Code: https://code.visualstudio.com/
   - Extensão "Remote - SSH"

---

## Passos para Acesso via Terminal

### 1. Autenticar no GCP:
```bash
gcloud auth login
gcloud config set project simprede-461309
```

### 2. Aceder à VM via SSH:
```bash
gcloud compute ssh simprede-airflow-vm --zone=europe-west1-b
```

> A primeira vez, o comando cria uma chave SSH automaticamente e envia-a para a VM.

---

## Acesso via VS Code (opcional)

### 1. Instalar extensão "Remote - SSH"

### 2. No VS Code:
- Abrir o Command Palette: `Ctrl+Shift+P`
- Escolher: `Remote-SSH: Connect to Host`
- Selecionar a entrada correta (ex: `simprede-airflow` ou criar uma)
- VS Code instala o servidor remoto na VM automaticamente (1ª vez)

---

## Configuração opcional do ficheiro SSH (Linux, macOS e Windows)

### Para Linux/macOS:
Editar (ou criar) o ficheiro `~/.ssh/config` e adicionar:

```bash
Host simprede-vm
    HostName 34.38.50.162
    User <teu_nome_utilizador_na_VM>
    IdentityFile ~/.ssh/google_compute_engine
```

### Para Windows (Git Bash, PowerShell ou WSL):
O ficheiro de configuração está em:

```bash
C:\Users\<teu_utilizador>\.ssh\config
```

E o caminho da chave privada será:

```bash
IdentityFile C:\Users\<teu_utilizador>\.ssh\google_compute_engine
```

> No Git Bash ou WSL, podes usar `~/.ssh` como no Linux.

Para saber o nome de utilizador, corre `whoami` após aceder à VM com `gcloud compute ssh`.

---

## Em caso de problemas:

Verifica:
- Estás no projeto correto (`gcloud config list`)?
- Tens as permissões IAM atribuídas?
- O IP da VM está correto?
- Estás a usar `gcloud compute ssh` na 1ª ligação e não SSH direto?

---
