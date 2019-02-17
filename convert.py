# Written by Nolan Cooper

import sys
import time
import datetime as dt
import pandas as pd
import json
import re
import os
import psutil
from pathlib import Path

if (len(sys.argv) < 2):
    print("Please provide a valid .csv filename")
else:
    filename = sys.argv[1]
    pattern = re.compile("^[a-zA-Z0-9_-]+(\.csv)$")
    matches = bool(pattern.match(filename))
    if (matches == False):
        print("Please provide a valid .csv filename")
    else:
        print("Getting data...")
        patent = []
        citations = []
        ages = []
        facebook = []
        twitter = []
        blogs = []
        news = []
        reddit = []
        googleplus = []

        US = []
        IT = []
        GB = []
        MX = []
        JP = []
        IN = []
        DE = []
        FR = []
        CA = []

        bachelors = []
        masters = []
        phd = []
        doctoral = []
        researcher = []
        associate_prof = []
        professor = []
        lecturer = []
        librarian = []

        engineering = []
        med_dent = []
        neuro = []
        chemistry = []
        sport = []
        ag_bio = []
        nursing = []
        biochem = []
        enviro = []
        pharm = []
        compsci = []
        business = []
        materials = []
        psych = []
        earth = []
        arts = []
        vet = []
        social = []
        econ = []
        math = []
        immune = []
        physics = []

        full_dataset_path = "D:\\Altmetrics\\combined_file.tar\\combined_file\\keys"
        directory_in_str = "C:\\Users\\Nolan\\Desktop\\keys2"
        pathlist = list(Path(full_dataset_path).glob('**/*.txt'))
        try:
            file_limit = int(sys.argv[2])
        except:
            file_limit = len(pathlist)
        start_time = time.perf_counter()

        for counter, path in enumerate(pathlist):
            if (counter > file_limit):
                break
            percent = (counter + 1) / file_limit * 100
            elapsed_time = int(time.perf_counter() - start_time)
            elapsed_str = str(dt.timedelta(seconds=elapsed_time))
            remaining_time = int((100 - percent) / percent * elapsed_time)
            remaining_str = str(dt.timedelta(seconds=remaining_time))
            process = psutil.Process(os.getpid())
            memory = process.memory_info().rss / 1024 / 1024
            output = "Processing file {} of {}, {:.1f}% complete, {} elapsed, {} remaining, using {:.2f}MB"
            output_built = output.format(counter, file_limit, percent, elapsed_str, remaining_str, memory)
            print(output_built, end='\r')
            path_in_str = str(path)
            with open(path_in_str) as f:
                for line in f:
                    record = json.loads(line)
                    try:
                        if (record['counts']['patent']['unique_users_count'] > 0):
                            patent.append(1)
                    except:
                        patent.append(0)

                    # Get popularity data
                    try:
                        citations.append(record['counts']['total']['posts_count'])
                    except:
                        citations.append(0)
                    try:
                        split1 = record['citation']['first_seen_on'].split("T")
                        split2 = split1[0].split("-")
                        pub_date = dt.date(int(split2[0]), int(split2[1]), int(split2[2]))
                        age = dt.date.today() - pub_date
                        age.days
                        ages.append(age.days)
                    except:
                        ages.append(0)
                    try:
                        facebook.append(record['counts']['facebook']['posts_count'])
                    except:
                        facebook.append(0)
                    try:
                        twitter.append(record['counts']['twitter']['posts_count'])
                    except:
                        twitter.append(0)
                    try:
                        blogs.append(record['counts']['blogs']['posts_count'])
                    except:
                        blogs.append(0)
                    try:
                        news.append(record['counts']['news']['posts_count'])
                    except:
                        news.append(0)
                    try:
                        reddit.append(record['counts']['reddit']['posts_count'])
                    except:
                        reddit.append(0)
                    try:
                        googleplus.append(record['counts']['googleplus']['posts_count'])
                    except:
                        googleplus.append(0)

                    # Get country data
                    try:
                        US.append(record['demographics']['geo']['mendeley']['US'])
                    except:
                        US.append(0)
                    try:
                        IT.append(record['demographics']['geo']['mendeley']['IT'])
                    except:
                        IT.append(0)
                    try:
                        GB.append(record['demographics']['geo']['mendeley']['GB'])
                    except:
                        GB.append(0)
                    try:
                        MX.append(record['demographics']['geo']['mendeley']['MX'])
                    except:
                        MX.append(0)
                    try:
                        JP.append(record['demographics']['geo']['mendeley']['JP'])
                    except:
                        JP.append(0)
                    try:
                        IN.append(record['demographics']['geo']['mendeley']['IN'])
                    except:
                        IN.append(0)
                    try:
                        DE.append(record['demographics']['geo']['mendeley']['DE'])
                    except:
                        DE.append(0)
                    try:
                        FR.append(record['demographics']['geo']['mendeley']['FR'])
                    except:
                        FR.append(0)
                    try:
                        CA.append(record['demographics']['geo']['mendeley']['CA'])
                    except:
                        CA.append(0)

                    # Get user data
                    try:
                        bachelors.append(record['demographics']['users']['mendeley']['by_status']['Student  > Bachelor'])
                    except:
                        bachelors.append(0)
                    try:
                        masters.append(record['demographics']['users']['mendeley']['by_status']['Student  > Master'])
                    except:
                        masters.append(0)
                    try:
                        phd.append(record['demographics']['users']['mendeley']['by_status']['Student  > Ph. D. Student'])
                    except:
                        phd.append(0)
                    try:
                        doctoral.append(record['demographics']['users']['mendeley']['by_status']['Student  > Doctoral Student'])
                    except:
                        doctoral.append(0)
                    try:
                        researcher.append(record['demographics']['users']['mendeley']['by_status']['Researcher'])
                    except:
                        researcher.append(0)
                    try:
                        associate_prof.append(record['demographics']['users']['mendeley']['by_status']['Professor > Associate Professor'])
                    except:
                        associate_prof.append(0)
                    try:
                        professor.append(record['demographics']['users']['mendeley']['by_status']['Professor'])
                    except:
                        professor.append(0)
                    try:
                        lecturer.append(record['demographics']['users']['mendeley']['by_status']['Lecturer'])
                    except:
                        lecturer.append(0)
                    try:
                        librarian.append(record['demographics']['users']['mendeley']['by_status']['Librarian'])
                    except:
                        librarian.append(0)

                    # Get field data
                    try:
                        engineering.append(record['demographics']['users']['mendeley']['by_discipline']['Engineering'])
                    except:
                        engineering.append(0)
                    try:
                        med_dent.append(record['demographics']['users']['mendeley']['by_discipline']['Medicine and Dentistry'])
                    except:
                        med_dent.append(0)
                    try:
                        neuro.append(record['demographics']['users']['mendeley']['by_discipline']['Neuroscience'])
                    except:
                        neuro.append(0)
                    try:
                        chemistry.append(record['demographics']['users']['mendeley']['by_discipline']['Chemistry'])
                    except:
                        chemistry.append(0)
                    try:
                        sport.append(record['demographics']['users']['mendeley']['by_discipline']['Sports and Recreations'])
                    except:
                        sport.append(0)
                    try:
                        ag_bio.append(record['demographics']['users']['mendeley']['by_discipline']['Agricultural and Biological Sciences'])
                    except:
                        ag_bio.append(0)
                    try:
                        nursing.append(record['demographics']['users']['mendeley']['by_discipline']['Nursing and Health Professions'])
                    except:
                        nursing.append(0)
                    try:
                        biochem.append(record['demographics']['users']['mendeley']['by_discipline']['Biochemistry, Genetics and Molecular Biology'])
                    except:
                        biochem.append(0)
                    try:
                        enviro.append(record['demographics']['users']['mendeley']['by_discipline']['Environmental Science'])
                    except:
                        enviro.append(0)
                    try:
                        pharm.append(record['demographics']['users']['mendeley']['by_discipline']['Pharmacology, Toxicology and Pharmaceutical Science'])
                    except:
                        pharm.append(0)
                    try:
                        compsci.append(record['demographics']['users']['mendeley']['by_discipline']['Computer Science'])
                    except:
                        compsci.append(0)
                    try:
                        business.append(record['demographics']['users']['mendeley']['by_discipline']['Business, Management and Accounting'])
                    except:
                        business.append(0)
                    try:
                        materials.append(record['demographics']['users']['mendeley']['by_discipline']['Materials Science'])
                    except:
                        materials.append(0)
                    try:
                        psych.append(record['demographics']['users']['mendeley']['by_discipline']['Psychology'])
                    except:
                        psych.append(0)
                    try:
                        earth.append(record['demographics']['users']['mendeley']['by_discipline']['Earth and Planetary Sciences'])
                    except:
                        earth.append(0)
                    try:
                        arts.append(record['demographics']['users']['mendeley']['by_discipline']['Arts and Humanities'])
                    except:
                        arts.append(0)
                    try:
                        vet.append(record['demographics']['users']['mendeley']['by_discipline']['Veterinary Science and Veterinary Medicine'])
                    except:
                        vet.append(0)
                    try:
                        social.append(record['demographics']['users']['mendeley']['by_discipline']['Social Sciences'])
                    except:
                        social.append(0)
                    try:
                        econ.append(record['demographics']['users']['mendeley']['by_discipline']['Economics, Econometrics and Finance'])
                    except:
                        econ.append(0)
                    try:
                        math.append(record['demographics']['users']['mendeley']['by_discipline']['Mathematics'])
                    except:
                        math.append(0)
                    try:
                        immune.append(record['demographics']['users']['mendeley']['by_discipline']['Immunology and Microbiology'])
                    except:
                        immune.append(0)
                    try:
                        physics.append(record['demographics']['users']['mendeley']['by_discipline']['Physics and Astronomy'])
                    except:
                        physics.append(0)

        print("\nPackaging data...")

        data = {
            'Patent Citation' : pd.Series(patent),
            'Age' : pd.Series(ages),
            'Total Citations': pd.Series(citations),
            'Tweets' : pd.Series(twitter),
            'News Mentions' : pd.Series(news),
            'Blog Posts' : pd.Series(blogs),
            'Reddit Posts' : pd.Series(reddit),
            'Facebook Posts' : pd.Series(facebook),
            'Google Plus Posts' : pd.Series(googleplus),
            'United States' : pd.Series(US),
            'Great Britain' : pd.Series(GB),
            'Germany' : pd.Series(DE),
            'France' : pd.Series(FR),
            'Canada' : pd.Series(CA),
            'Italy' : pd.Series(IT),
            'Japan' : pd.Series(JP),
            'India' : pd.Series(IN),
            'Mexico' : pd.Series(MX),
            "Bachelors" : pd.Series(bachelors),
            "Masters" : pd.Series(masters),
            "PhD" : pd.Series(phd),
            "Doctoral" : pd.Series(doctoral),
            "Researcher" : pd.Series(researcher),
            "Associate Prof." : pd.Series(associate_prof),
            "Professor" : pd.Series(professor),
            "Lecturer" : pd.Series(lecturer),
            "Librarian" : pd.Series(librarian),
            'Medicine and Dentistry' : pd.Series(med_dent),
            'Agricultural and Biological Sciences' : pd.Series(ag_bio),
            'Biochemistry, Genetics and Molecular Biology' : pd.Series(biochem),
            'Pharmacology, Toxicology and Pharmaceutical Science' : pd.Series(pharm),
            'Immunology and Microbiology' : pd.Series(immune),
            'Nursing and Health Professions' : pd.Series(nursing),
            'Veterinary Science and Veterinary Medicine' : pd.Series(vet),
            'Social Sciences' : pd.Series(social),
            'Mathematics' : pd.Series(math),
            'Neuroscience' : pd.Series(neuro),
            'Arts and Humanities' : pd.Series(arts),
            'Chemistry' : pd.Series(chemistry),
            'Materials Science' : pd.Series(materials),
            'Environmental Science' : pd.Series(enviro),
            'Psychology' : pd.Series(psych),
            'Earth and Planetary Sciences' : pd.Series(earth),
            'Engineering' : pd.Series(engineering),
            'Physics and Astronomy' : pd.Series(physics),
            'Computer Science' : pd.Series(compsci),
            'Business, Management and Accounting' : pd.Series(business),
            'Economics, Econometrics and Finance' : pd.Series(econ),
            'Sports and Recreations' : pd.Series(sport)
           }

        df = pd.DataFrame(data)
        size = "Created DataFrame with {} records"
        print(size.format(len(df.index)))
        print("Writing data to file...")
        df.to_csv(filename)
