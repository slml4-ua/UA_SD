"""Pantalla de texto del panel de WM_Central.

Dibuja el panel con colores ANSI. En una terminal interactiva se redibuja en
su sitio cada segundo sin molestar a lo que el usuario esté escribiendo; sin
terminal (por ejemplo en los logs de un servidor) se imprime solo cuando cambia.
"""

import os
import shutil
import sys
import threading

RESET = "\033[0m"
NEGRITA = "\033[1m"
VERDE = "\033[30;42m"
VERDE_CLARO = "\033[30;102m"
ROJO = "\033[97;41m"
NARANJA = "\033[30;48;5;208m"
GRIS = "\033[97;100m"

# En Windows hay que activar el procesamiento de secuencias ANSI de la consola
os.system("")


def ancho_util(maximo=60):
    columnas = shutil.get_terminal_size((80, 24)).columns
    return max(20, min(maximo, columnas - 1))


def linea(texto, ancho, color=""):
    """Recorta/rellena a `ancho` caracteres y aplica un color de fondo."""
    texto = texto[:ancho].ljust(ancho)
    return f"{color}{texto}{RESET}" if color else texto


class Pantalla:
    def __init__(self):
        self.interactiva = sys.stdout.isatty() and sys.stdin.isatty()
        self.lock = threading.Lock()
        self.altura = 0
        self.activa = True

    def imprimir(self, lineas):
        """Imprime el panel entero (hace scroll) y recuerda su altura."""
        with self.lock:
            sys.stdout.write("\n".join(lineas) + "\n")
            sys.stdout.flush()
            self.altura = len(lineas)
            self.activa = True

    def refrescar(self, lineas):
        """Redibuja el panel en el mismo sitio, respetando el cursor.

        Devuelve False si el panel ha cambiado de altura y hay que reimprimirlo.
        """
        with self.lock:
            if not self.activa:
                return True  # el menú está en uso: no se toca la pantalla
            if len(lineas) != self.altura:
                return False
            salida = ["\0337", f"\033[{self.altura}A\r"]
            for texto in lineas:
                salida.append(texto + "\033[K\n")
            salida.append("\0338")
            sys.stdout.write("".join(salida))
            sys.stdout.flush()
            return True

    def pausar(self):
        with self.lock:
            self.activa = False