#1) FileCatalog Event Number Check: Check runs in FileCatalog database after run 46619 with > 1 million events
def get_all_run_numbers(cursor):
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

#Note -- there are two run numbers as the timeline:
#1) After run 46619 we changed to 0 crossing angle
#2) After 47289, we updated the trigger firmware

#2) Calo Status Check: Check the emcal_auto, ihcal_auto, ohcal_auto status to be 'golden' in Production_write database. 
#And get the intersection with the first step

def get_emcal_auto_golden_run_numbers(file_cataLog_run_numbers, production_cursor):
  query = """
  SELECT runnumber
  FROM goodruns
  WHERE (emcal_auto).runclass = 'GOLDEN'
  """
  production_cursor.execute(query)
  emcal_auto_golden_run_numbers = {row.runnumber for row in production_cursor.fetchall()}
  return list(
    emcal_auto_golden_run_numbers.intersection(
        set(file_cataLog_run_numbers)
    )
  )

#3 Get the runs pass all previous checks
#get the intersection of all sets
golden_run_numbers = list(
    set(emcal_auto_golden_run_numbers).intersection(
        set(ihcal_auto_golden_run_numbers),
      set(ohcal_auto_golden_run_numbers),
      set(file_catalog_run_numbers)
    )
)
golden_run_numbers.sort()
with open(;runnumberlist_calo.txt', 'w') as f:
    for run_number in golden_run_numbers:
        f.write(f"{run_number}\n")
print(f"Number of GOLDEN runs saved to runnumberlist_calo.txt: {len(golden_run_numbers)}")


#GL1 MISMATCH CHECK: read the even number for each run from database
while IFS = read -r runnumber; do
    echo "Processing runnumber: $runnumber"
    psql -h sphnxdaqbreplica daq -t -A -F" " -c "SELECT * FROM event_numbers WHERE runnumber = $runnumber;" >> "$output_file"
done < "$input_file"

#5) Check the event number and gl1mismatch:
  #a) Check there are 1,000,000 events in gl1daq.
  #b) gl1daq and SEB00 to SEB17 exists.
  #c) gl1daq has 1 more events compared to other seb.
  #d) If there is one or two sebs have a bit less (<500 events) than gl1daq, it’s acceptable.


#FEM Mismatch Check: Read the FEM clock for each SEB machine with ddump
ddump -i -e 10 /sphenix/lustre01/sphnxpro/physics/emcal/physics/physics_seb0$i-000$runnumber-0000.prdf | grep 'FEM Clock:' | sed 's/[^0-9 ]//g' ?? "$output_file"

#7: Check the FEM clock to ensure that for each IB in the same SEB machine, the FEM clock value is the same


"""
Summary From Hanpus Slides:
1) The golden run list generation for now checks the Calo status, event numbers, gl1 mismatch and FEM mismatch to ensure the run is good.
2) The gl1 and FEM mismatch problem is supposed to be fixed by event combiner in the future (maybe now).
3) The Calo status also influenced by the gl1 and FEM mismatch, so in the future, we should have much more golden runs.
4) For now, there are some problems in the newest production ana430_2024p007. Before Run 48500, the production is not very good. And the output DST files are not always in the correct directory. CreateDstList cannot always give the right list.
5) The golden run is in /sphenix/user/hanpuj/CaloDataAna24/dst_list/data/runnumber.txt
The associated DST lists are in /sphenix/user/hanpuj/CaloDataAna24/dst_list/data/dst_list
"""
