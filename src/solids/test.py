import subprocess

command = ["java", "/Users/martimpassos/dev/situated-views-etl/src/iiif-tiler-0.9.6/src/main/java/uk/co/gdmrdigital/iiif/image/Tiler.java",
           "/Users/martimpassos/Desktop/tiles/default.jpeg"]

subprocess.Popen(
    command,
    shell=True,
    # cwd="src/iiif-tiler-0.9.6/src/main/java/uk/co/gdmrdigital/iiif",
    # stdout=subprocess.PIPE,
    # stderr=subprocess.PIPE,
)

# process.communicate()
