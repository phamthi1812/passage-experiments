import os
import shutil
import subprocess
# Define the old and new directories
old_dir = "/GDD/Thi/sage-jena-benchmarks/queries/wdbench-multiple-tps"
new_dir = old_dir + "_cartesian"
jar_file = "/GDD/Thi/sage-jena-benchmarks/executables/is-cartesian-jar-with-dependencies.jar"

if not os.path.exists(new_dir):
    os.makedirs(new_dir)

def is_cartesian(file_path):
    try:
        result = subprocess.run(['java', '-jar', jar_file, '--file=' + file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output_lines = result.stdout.decode('utf-8').splitlines()

        # Check if "cartesian" is in the output of is-cartesian-jar-with-dependencies.jar
        return output_lines[-1].strip() == "True"
    except Exception as e:
        print(f"Error running JAR for {file_path}: {e}")
        return False

# Process all sparql files in the old directory
for file_name in os.listdir(old_dir):
    if file_name.endswith(".sparql"):
        file_path = os.path.join(old_dir, file_name)
        if is_cartesian(file_path):
            new_file_name = file_name + ".cartesian"
            new_file_path = os.path.join(new_dir, new_file_name)
            shutil.move(file_path, new_file_path)

print("Process completed.")