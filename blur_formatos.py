from PIL import Image, ImageFilter
import os

folder = "formatos_pdf"

for file in os.listdir(folder):
    if file.lower().endswith(".png"):
        path = os.path.join(folder, file)

        img = Image.open(path)
        w, h = img.size

        # zona superior (datos de cuenta)
        top = img.crop((0, int(h*0.18), w, int(h*0.35)))
        top = top.filter(ImageFilter.GaussianBlur(25))
        img.paste(top, (0, int(h*0.18)))

        # zona movimientos
        bottom = img.crop((0, int(h*0.55), w, h))
        bottom = bottom.filter(ImageFilter.GaussianBlur(25))
        img.paste(bottom, (0, int(h*0.55)))

        img.save(path)

print("Imágenes blureadas correctamente")