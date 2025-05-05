import os
import qrcode
from PIL import Image


def change_qr_code_color(input_path, output_path, target_color=(0, 255, 0)):
    # Open the QR code image
    image = Image.open(input_path).convert("RGBA")

    # Get the image data
    data = image.getdata()

    # Create a new image with green QR code
    new_data = [(target_color[0], target_color[1], target_color[2], pixel[3]) for pixel in data]

    # Update the image data
    image.putdata(new_data)

    # Save the new image
    image.save(output_path, "PNG")


if __name__ == "__main__":
    input_directory = "qrcodes"
    output_directory = "qr_codes_green"
    target_color = (0, 255, 0)

    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Loop through all files in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith(".png"):
            input_path = os.path.join(input_directory, filename)
            output_path = os.path.join(output_directory, f"{filename[:-4]}_green.png")

            # Change the color of the QR code
            change_qr_code_color(input_path, output_path, target_color)
