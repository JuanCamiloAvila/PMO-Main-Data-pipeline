import gspread
from google.oauth2.credentials import Credentials
import traceback

def probar_conexion():
    print("🔄 Iniciando prueba de conexión con Google Drive...")
    
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        # 1. Conexión
        creds = Credentials.from_authorized_user_file('token.json', scopes)
        gc = gspread.authorize(creds)
        
        # 2. Obtener lista de archivos
        print("🌐 Conectando con los servidores de Google...\n")
        archivos = gc.list_spreadsheet_files()
        
        print(f"✅ ¡Conexión exitosa! Pude ver {len(archivos)} archivos de Google Sheets.\n")
        
        # 3. Imprimir los primeros 20 archivos
        print("📋 Listando los primeros 20 archivos:")
        print("-" * 60)
        for i, archivo in enumerate(archivos[:20], 1):
            print(f"{i}. 📄 {archivo['name']}")
            print(f"   🔗 ID: {archivo['id']}")
            print("-" * 60)
            
        print("\n💡 Si ves tus archivos corporativos aquí, el token tiene los permisos correctos.")
        
    except Exception as e:
        print("❌ Ocurrió un error al intentar conectar:")
        traceback.print_exc()

if __name__ == "__main__":
    probar_conexion()