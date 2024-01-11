import glob
import os
import shutil
import time


def run_generation(tmp_folder, db_generator_folder, db_script, dbsize, output_folder):
    # clean the tmp folder
    shutil.rmtree(tmp_folder, ignore_errors=True)
    os.mkdir(tmp_folder)
    # copy the database_generator folder
    shutil.copytree(db_generator_folder, tmp_folder+"/database_generator")
    # copy the db_script
    shutil.copy(db_script, tmp_folder+"/db_script.py")
    # execute the db_script and generate the database
    os.chdir(tmp_folder)
    os.system("python db_script.py "+dbsize)
    # move the generated database to output_folder
    os.makedirs(output_folder, exist_ok=True)
    shutil.copytree("output", output_folder, dirs_exist_ok=True)


dir_path = os.path.dirname(os.path.realpath(__file__))
database_generator_util_path = dir_path + "/database_generator"
database_output_folder = dir_path + "/databases"

leetcode_uniform_scripts = glob.glob(
    dir_path + "/leetcode/leetcode_uniform/*.py")
leetcode_zipf_scripts = glob.glob(dir_path + "/leetcode/leetcode_zipf/*.py")
calcite_uniform_scripts = glob.glob(dir_path + "/calcite/calcite_uniform/*.py")
calcite_zipf_scripts = glob.glob(dir_path + "/calcite/calcite_zipf/*.py")

# need a tmp folder to assemble the scripts
tmp_folder = dir_path + "/tmp_"+str(int(time.time()))

# generating leetcode uniform csv files
for idx, script in enumerate(leetcode_uniform_scripts):
    for db_size in ["100K", "1M"]:
        script_name = script.split("/")[-1]
        problem_id = script_name.split(".")[0]
        print("generating uniform leetcode uniform csv files for problem " +
              problem_id + " with size "+db_size + " ("+str(idx+1)+"/"+str(len(leetcode_uniform_scripts))+")")
        run_generation(tmp_folder, database_generator_util_path, script, db_size,
                       database_output_folder + "/leetcode_uniform/" + db_size + "/" + problem_id)

# generating leetcode zipf csv files
for idx, script in enumerate(leetcode_zipf_scripts):
    for db_size in ["100K", "1M"]:
        script_name = script.split("/")[-1]
        problem_id = script_name.split(".")[0]
        print("generating uniform leetcode zipf csv files for problem " +
              problem_id + " with size "+db_size + " ("+str(idx+1)+"/"+str(len(leetcode_zipf_scripts))+")")
        run_generation(tmp_folder, database_generator_util_path, script, db_size,
                       database_output_folder + "/leetcode_zipf/" + db_size + "/" + problem_id)

# generating calcite uniform csv files
for idx, script in enumerate(calcite_uniform_scripts):
    for db_size in ["calcite_250K", "calcite_1M", "calcite_4M"]:
        script_name = script.split("/")[-1]
        problem_id = script_name.split(".")[0]
        print("generating calcite uniform csv files for problem " +
              problem_id + " with size "+db_size + " ("+str(idx+1)+"/"+str(len(calcite_uniform_scripts))+")")
        run_generation(tmp_folder, database_generator_util_path, script, db_size,
                       database_output_folder + "/calcite_uniform/" + db_size + "/" + problem_id)

# generating calcite zipf csv files
for idx, script in enumerate(calcite_zipf_scripts):
    for db_size in ["calcite_250K", "calcite_1M", "calcite_4M"]:
        script_name = script.split("/")[-1]
        problem_id = script_name.split(".")[0]
        print("generating calcite zipf csv files for problem " +
              problem_id + " with size "+db_size + " ("+str(idx+1)+"/"+str(len(calcite_zipf_scripts))+")")
        run_generation(tmp_folder, database_generator_util_path, script, db_size,
                       database_output_folder + "/calcite_zipf/" + db_size + "/" + problem_id)

# finally, remove tmp folder
shutil.rmtree(tmp_folder, ignore_errors=True)
