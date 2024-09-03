import pyodbc

def get_run_numbers(cursor):
    """
    Query the FileCatalog database to get unique run numbers
    with at least 1 million total events and a run number greater than 46619.
    """
    query = """
    SELECT runnumber
    FROM datasets
    WHERE filename like 'DST_CALO%'
    GROUP BY runnumber
    HAVING SUM(events) >= 1000000 AND runnumber > 46619;
    """
    cursor.execute(query)
    run_numbers = [row.runnumber for row in cursor.fetchall()]
    return run_numbers

def get_emcal_auto_golden_run_numbers(file_catalog_run_numbers, production_cursor):
    """
    Check the emcal_auto status to be 'GOLDEN' in the Production database
    and get the intersection with file_catalog_run_numbers.
    """
    query = """
    SELECT runnumber
    FROM goodruns
    WHERE (emcal_auto).runclass = 'GOLDEN'
    """
    production_cursor.execute(query)
    emcal_auto_golden_run_numbers = {row.runnumber for row in production_cursor.fetchall()}
    return list(emcal_auto_golden_run_numbers.intersection(set(file_catalog_run_numbers)))

def get_ihcal_auto_golden_run_numbers(file_catalog_run_numbers, production_cursor):
    """
    Check the ihcal_auto status to be 'GOLDEN' in the Production database
    and get the intersection with file_catalog_run_numbers.
    """
    query = """
    SELECT runnumber
    FROM goodruns
    WHERE (ihcal_auto).runclass = 'GOLDEN'
    """
    production_cursor.execute(query)
    ihcal_auto_golden_run_numbers = {row.runnumber for row in production_cursor.fetchall()}
    return list(ihcal_auto_golden_run_numbers.intersection(set(file_catalog_run_numbers)))

def get_ohcal_auto_golden_run_numbers(file_catalog_run_numbers, production_cursor):
    """
    Check the ohcal_auto status to be 'GOLDEN' in the Production database
    and get the intersection with file_catalog_run_numbers.
    """
    query = """
    SELECT runnumber
    FROM goodruns
    WHERE (ohcal_auto).runclass = 'GOLDEN'
    """
    production_cursor.execute(query)
    ohcal_auto_golden_run_numbers = {row.runnumber for row in production_cursor.fetchall()}
    return list(ohcal_auto_golden_run_numbers.intersection(set(file_catalog_run_numbers)))

def filter_golden_runs(file_catalog_run_numbers, production_cursor):
    """
    Filter run numbers based on the 'GOLDEN' status of emcal_auto, ihcal_auto, and ohcal_auto.
    Only runs that pass all these checks will be considered 'GOLDEN'.
    """
    emcal_auto_golden_runs = get_emcal_auto_golden_run_numbers(file_catalog_run_numbers, production_cursor)
    ihcal_auto_golden_runs = get_ihcal_auto_golden_run_numbers(file_catalog_run_numbers, production_cursor)
    ohcal_auto_golden_runs = get_ohcal_auto_golden_run_numbers(file_catalog_run_numbers, production_cursor)

    # Intersection of all sets to get the final golden run numbers
    golden_run_numbers = list(
        set(emcal_auto_golden_runs).intersection(
            set(ihcal_auto_golden_runs),
            set(ohcal_auto_golden_runs),
            set(file_catalog_run_numbers)
        )
    )
    
    golden_run_numbers.sort()  # Optional: Sort the run numbers
    return golden_run_numbers

def check_gl1_mismatch(input_file, output_file):
    """
    Perform the GL1 Mismatch Check:
    - Read the event number for each run from the database.
    - Check the GL1 Mismatch conditions for each run.
    """
    with open(input_file, 'r') as f:
        for runnumber in f:
            runnumber = runnumber.strip()
            print(f"Processing runnumber: {runnumber}")
            command = f"psql -h sphnxdaqbreplica daq -t -A -F\" \" -c \"SELECT * FROM event_numbers WHERE runnumber = {runnumber};\" >> \"{output_file}\""
            os.system(command)

def check_fem_mismatch(output_file):
    """
    Perform the FEM Mismatch Check:
    - Read the FEM clock for each SEB machine with ddump.
    - Ensure that for each IB in the same SEB machine, the FEM clock value is the same.
    """
    with open(output_file, 'r') as f:
        for line in f:
            runnumber = line.split()[0]
            command = f"ddump -i -e 10 /sphenix/lustre01/sphnxpro/physics/emcal/physics/physics_seb0$i-000{runnumber}-0000.prdf | grep 'FEM Clock:' | sed 's/[^0-9 ]//g' >> \"{output_file}\""
            os.system(command)

def main():
    # Connect to the FileCatalog database
    file_catalog_conn = pyodbc.connect("DSN=FileCatalog;UID=phnxrc;READONLY=True")
    file_catalog_cursor = file_catalog_conn.cursor()

    # Get unique run numbers with at least 1 million total events
    file_catalog_run_numbers = get_run_numbers(file_catalog_cursor)
    print(f"Number of runs found in the File Catalog: {len(file_catalog_run_numbers)}")

    # Close the FileCatalog database connection
    file_catalog_conn.close()

    # Connect to the Production database
    production_conn = pyodbc.connect("DSN=Production_write")
    production_cursor = production_conn.cursor()

    # Filter run numbers based on 'GOLDEN' status
    golden_run_numbers = filter_golden_runs(file_catalog_run_numbers, production_cursor)

    # Save golden run numbers to a text file at the specified path
    output_path = '/sphenix/user/patsfan753/analysis/Jet-Study/runnumberlist_calo.txt'
    with open(output_path, 'w') as f:
        for run_number in golden_run_numbers:
            f.write(f"{run_number}\n")
    print(f"Number of GOLDEN runs saved to {output_path}: {len(golden_run_numbers)}")

    # Perform GL1 Mismatch Check
    check_gl1_mismatch(output_path, 'gl1_mismatch_output.txt')

    # Perform FEM Mismatch Check
    check_fem_mismatch('fem_mismatch_output.txt')

    # Close the Production database connection
    production_conn.close()

if __name__ == "__main__":
    main()
