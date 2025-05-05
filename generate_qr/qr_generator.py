import qrcode
from PIL import Image, ImageDraw, ImageFont
import os


def generate_qr_codes_with_numbers(numbers, output_folder='qrcodes'):

    font_size = 17

    # Create a custom font
    font_path = "Aileron-Bold.otf"  # Replace with the path to your TTF font file
    custom_font = ImageFont.truetype(font_path, size=font_size)

    for num in numbers:
        data = str(num)

        # Generate QR code
        qr = qrcode.make(data)

        # Create an output folder if it doesn't exist
        import os
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Save QR code image
        qr_path = f"{output_folder}/qrcode_{data}.png"
        qr.save(qr_path)

        # Create an image with the number below the QR code
        img = Image.open(qr_path)
        draw = ImageDraw.Draw(img)
        font = ImageFont.load_default()

        # Calculate position to center the number below the QR code
        _, _, width, height = draw.textbbox((0,0), text=data, font=custom_font, align="center")
        x = (img.width - width) // 2
        y = height

        # Draw the number below the QR code
        draw.text((x, y), data, fill="black", font=custom_font)

        # Save the final image
        img.save(qr_path)


def hex_to_rgb(hex_color):
    # Convert hexadecimal color code to RGB
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))


def change_qr_code_color(input_path, output_path, target_color="#3AB648"):
    # Convert hexadecimal color code to RGB
    target_color_rgb = hex_to_rgb(target_color)

    # Open the QR code image
    image = Image.open(input_path).convert("RGBA")

    # Get the image data
    data = image.getdata()

    # Create a new image with the specified color for the QR code and original background color
    new_data = []
    for pixel in data:
        if pixel[:3] == (0, 0, 0):  # Check if the pixel is part of the QR code (black)
            new_data.append((target_color_rgb[0], target_color_rgb[1], target_color_rgb[2], pixel[3]))
        else:
            new_data.append(pixel)

    # Update the image data
    image.putdata(new_data)

    # Save the new image
    image.save(output_path, "PNG")


if __name__ == "__main__":
    numbers_to_generate = [1000101, 1000102, 1000103, 1000104, 1000105, 1000106, 1000107, 1000108, 1000109, 1000110,
                           1000111, 1000112, 1000113, 1000114, 1000115, 1000116, 1000117, 1000118, 1000119, 1000120,
                           1000121, 1000122, 1000123, 1000124, 1000125, 1000126, 1000127, 1000128, 1000129, 1000130,
                           1000131, 1000132, 1000133, 1000134, 1000135, 1000136, 1000137, 1000138, 1000139, 1000140,
                           1000141, 1000142, 1000143, 1000144, 1000145, 1000146, 1000147, 1000148, 1000149, 1000150,
                           1000151, 1000152, 1000153, 1000154, 1000155, 1000156, 1000157, 1000158, 1000159, 1000160,
                           1000161, 1000162, 1000163, 1000164, 1000165, 1000166, 1000167, 1000168, 1000169, 1000170,
                           1000171, 1000172, 1000173, 1000174, 1000175, 1000176, 1000177, 1000178, 1000179, 1000180,
                           1000181, 1000182, 1000183, 1000184, 1000185, 1000186, 1000187, 1000188, 1000189, 1000190,
                           1000191, 1000192, 1000193, 1000194, 1000195, 1000196, 1000197, 1000198, 1000199, 1000200,
                           1000201, 1000202, 1000203, 1000204, 1000205, 1000206, 1000207, 1000208, 1000209, 1000210,
                           1000211, 1000212, 1000213, 1000214, 1000215, 1000216, 1000217, 1000218, 1000219, 1000220,
                           1000221, 1000222, 1000223, 1000224, 1000225, 1000226, 1000227, 1000228, 1000229, 1000230,
                           1000231, 1000232, 1000233, 1000234, 1000235, 1000236, 1000237, 1000238, 1000239, 1000240,
                           1000241, 1000242, 1000243, 1000244, 1000245, 1000246, 1000247, 1000248, 1000249, 1000250,
                           1000251, 1000252, 1000253, 1000254, 1000255, 1000256, 1000257, 1000258, 1000259, 1000260,
                           1000261, 1000262, 1000263, 1000264, 1000265, 1000266, 1000267, 1000268, 1000269, 1000270,
                           1000271, 1000272, 1000273, 1000274, 1000275, 1000276, 1000277, 1000278, 1000279, 1000280,
                           1000281, 1000282, 1000283, 1000284, 1000285, 1000286, 1000287, 1000288, 1000289, 1000290,
                           1000291, 1000292, 1000293, 1000294, 1000295, 1000296, 1000297, 1000298, 1000299, 1000300,
                           1000301, 1000302, 1000303, 1000304, 1000305, 1000306, 1000307, 1000308, 1000309, 1000310,
                           1000311, 1000312, 1000313, 1000314, 1000315, 1000316, 1000317, 1000318, 1000319, 1000320,
                           1000321, 1000322, 1000323, 1000324, 1000325, 1000326, 1000327, 1000328, 1000329, 1000330,
                           1000331, 1000332, 1000333, 1000334, 1000335, 1000336, 1000337, 1000338, 1000339, 1000340,
                           1000341, 1000342, 1000343, 1000344, 1000345, 1000346, 1000347, 1000348, 1000349, 1000350,
                           1000351, 1000352, 1000353, 1000354, 1000355, 1000356, 1000357, 1000358, 1000359, 1000360,
                           1000361, 1000362, 1000363, 1000364, 1000365, 1000366, 1000367, 1000368, 1000369, 1000370,
                           1000371, 1000372, 1000373, 1000374, 1000375, 1000376, 1000377, 1000378, 1000379, 1000380,
                           1000381, 1000382, 1000383, 1000384, 1000385, 1000386, 1000387, 1000388, 1000389, 1000390,
                           1000391, 1000392, 1000393, 1000394, 1000395, 1000396, 1000397, 1000398, 1000399, 1000400,
                           1000401, 1000402, 1000403, 1000404, 1000405, 1000406, 1000407, 1000408, 1000409, 1000410,
                           1000411, 1000412, 1000413, 1000414, 1000415, 1000416, 1000417, 1000418, 1000419, 1000420,
                           1000421, 1000422, 1000423, 1000424, 1000425, 1000426, 1000427, 1000428, 1000429, 1000430,
                           1000431, 1000432, 1000433, 1000434, 1000435, 1000436, 1000437, 1000438, 1000439, 1000440,
                           1000441, 1000442, 1000443, 1000444, 1000445, 1000446, 1000447, 1000448, 1000449, 1000450]
    generate_qr_codes_with_numbers(numbers_to_generate)
    input_directory = "qrcodes"
    output_directory = "qr_codes_green"
    target_color = "#3AB648"

    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # Loop through all files in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith(".png"):
            input_path = os.path.join(input_directory, filename)
            output_path = os.path.join(output_directory, f"{filename[:-4]}_green.png")

            # Change the color of the QR code
            change_qr_code_color(input_path, output_path, target_color)
