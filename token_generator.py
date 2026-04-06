import os
from google_auth_oauthlib.flow import InstalledAppFlow

# Los permisos que necesita el script
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def main():
    print("🌐 Abriendo el navegador para iniciar sesión...")
    # Lee el archivo que acabas de renombrar
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    
    # Esto abrirá una ventana en tu navegador web. 
    creds = flow.run_local_server(port=0)

    # Guarda tu pase especial en un archivo nuevo
    with open('token.json', 'w') as token_file:
        token_file.write(creds.to_json())
        
    print("✅ ¡Éxito! Se ha creado el archivo 'token.json'.")

if __name__ == '__main__':
    main()