-- Cherry Mock Data: 500+ torrents with files
-- Run: psql -h localhost -U postgres -d cherry -f mock_data.sql

DO $$
DECLARE
    i INTEGER;
    total_cats INTEGER := 7;
    cat_idx INTEGER;
    base_name TEXT;
    release_name TEXT;
    info_hash TEXT;
    piece_len INTEGER;
    total_len BIGINT;
    file_cnt INTEGER;
    tid BIGINT;
    file_idx INTEGER;
    created_age INTERVAL;
    piece_lens INTEGER[] := ARRAY[262144, 524288, 1048576, 2097152, 4194304];
    movie_names TEXT[] := ARRAY['The.Secret.Garden','Lost.in.Translation','Eternal.Sunshine','Blade.Runner.2049','Interstellar','Inception','The.Matrix.Resurrections','Dune.Part.Two','Oppenheimer','Everything.Everywhere.All.At.Once','Parasite','The.Grand.Budapest.Hotel','Mad.Max.Fury.Road','Whiplash','Arrival','Get.Out','A.Quiet.Place','Spider-Man.Across.The.Spider-Verse','The.Batman','Top.Gun.Maverick','John.Wick.Chapter.4','Mission.Impossible.Dead.Reckoning','Avatar.The.Way.of.Water','Black.Panther.Wakanda.Forever','Guardians.of.the.Galaxy.Vol.3','The.Flash','Barbie','No.Time.to.Die','Tenet','Dunkirk','The.Dark.Knight','Pulp.Fiction','Fight.Club','Forrest.Gump','The.Shawshank.Redemption','Goodfellas','The.Godfather','Schindlers.List','Saving.Private.Ryan','Gladiator'];
    tv_names TEXT[] := ARRAY['Breaking.Bad.S05','Better.Call.Saul.S06','Stranger.Things.S04','The.Mandalorian.S03','The.Last.of.Us.S01','House.of.the.Dragon.S01','The.Boys.S03','Succession.S04','Severance.S01','The.White.Lotus.S02','Ted.Lasso.S03','The.Bear.S02','Yellowstone.S05','The.Crown.S06','Black.Mirror.S06','Fargo.S05','True.Detective.S04','The.Witcher.S03','Upload.S03','Silo.S01','Foundation.S02','Invincible.S02','Arcane.S02','Rick.and.Morty.S07','The.Simpsons.S35','Family.Guy.S22','South.Park.S27','Futurama.S11','One.Piece.S21','Attack.on.Titan.S04'];
    music_names TEXT[] := ARRAY['Taylor.Swift.Midnights','Kendrick.Lamar.DAMN','Radiohead.In.Rainbows','Daft.Punk.Random.Access.Memories','Pink.Floyd.The.Dark.Side.of.the.Moon','Fleetwood.Mac.Rumours','Nirvana.Nevermind','Kanye.West.Graduation','Billie.Eilish.Happier.Than.Ever','Tame.Impala.Currents','Arctic.Monkeys.AM','Lana.Del.Rey.Norman.Fcking.Rockwell','Frank.Ocean.Blonde','Tyler.the.Creator.IGOR','Mac.Miller.Circles','Adele.30','The.Weeknd.After.Hours','Olivia.Rodrigo.GUTS','Harry.Styles.Harrys.House','Bad.Bunny.Un.Verano.Sin.Ti','Beyonce.Renaissance','Drake.Certified.Lover.Boy','Post.Malone.Hollywoods.Bleeding','Ed.Sheeran.Divide','Bruno.Mars.24K.Magic'];
    software_names TEXT[] := ARRAY['Ubuntu.24.04.LTS.Desktop','Debian.12.Bookworm','Fedora.39.Workstation','Linux.Mint.21.3.Cinnamon','Windows.11.Pro.23H2','Microsoft.Office.2024.Pro.Plus','Adobe.Photoshop.2025','Adobe.Premiere.Pro.2025','AutoCAD.2025','MATLAB.R2024b','Visual.Studio.2024.Enterprise','JetBrains.IntelliJ.IDEA.2024','VMware.Workstation.Pro.17','Docker.Desktop.4.30','Postman.11','Blender.4.1','Unreal.Engine.5.4','Unity.2024','Godot.4.3','Node.js.22.LTS','Python.3.13','Go.1.23','Rust.1.80','Cyberpunk.2077.Phantom.Liberty','Elden.Ring.Shadow.of.the.Erdtree','Baldurs.Gate.3','Starfield','Hogwarts.Legacy','Red.Dead.Redemption.2','The.Legend.of.Zelda.Tears.of.the.Kingdom'];
    book_names TEXT[] := ARRAY['The.Hitchhikers.Guide.to.the.Galaxy','Dune.Frank.Herbert','Neuromancer.William.Gibson','Snow.Crash.Neal.Stephenson','The.Three-Body.Problem.Cixin.Liu','Project.Hail.Mary.Andy.Weir','A.Game.of.Thrones.GRRM','The.Name.of.the.Wind.Patrick.Rothfuss','The.Way.of.Kings.Brandon.Sanderson','The.Hunger.Games.Suzanne.Collins','Clean.Code.Robert.C.Martin','Designing.Data-Intensive.Applications','The.Pragmatic.Programmer','Structure.and.Interpretation.of.Computer.Programs','Introduction.to.Algorithms.CLRS','Deep.Learning.Ian.Goodfellow','Atomic.Habits.James.Clear','Sapiens.Yuval.Noah.Harari','Educated.Tara.Westover','Becoming.Michelle.Obama'];
    misc_names TEXT[] := ARRAY['Brazzers.Exxtra.2024','Reality.Kings.Premium','BangBros.Collection','Naughty.America.Latest','OnlyFans.Leaks.Pack','Mofos.Behind.the.Scenes','Fake.Taxi.Full.Series','Public.Agent.Compilation','X-Art.4K.Pack','Met-Art.Collection','Vixen.Studios.UHD','Tushy.Raw.4K','Blacked.Raw.Compilation','JAV.Premium.Pack.2024','HEYZO.Full.HD','Caribbeancom.Premium','Toky-Hot.Collection','FC2-PPV.Series','1Pondo.4K.Pack'];
    anime_names TEXT[] := ARRAY['Demon.Slayer.S04','Jujutsu.Kaisen.S02','Chainsaw.Man.S01','Spy.x.Family.S02','One.Punch.Man.S03','Mob.Psycho.100.S03','Fullmetal.Alchemist.Brotherhood','Death.Note.Complete','Cowboy.Bebop.Complete','Neon.Genesis.Evangelion','Steins.Gate.Complete','Code.Geass.Complete','Hunter.x.Hunter.2011','Haikyuu.Complete','Vinland.Saga.S02','Made.in.Abyss.S02','Mushoku.Tensei.S02','Re.Zero.S02','Overlord.S04','That.Time.I.Got.Reincarnated.as.a.Slime.S03','KonoSuba.S03','Oshi.no.Ko.S01','Bocchi.the.Rock.S01','Frieren.Beyond.Journeys.End','Solo.Leveling.S01'];
    quality_opts TEXT[] := ARRAY['1080p','2160p','720p','1080p.BluRay','2160p.BluRay','1080p.WEB-DL','2160p.WEB-DL','1080p.WEBRip','2160p.HDR','1080p.HEVC','2160p.HEVC','1080p.x264','1080p.x265','2160p.x265','BDRip','BRRip','HDTV','WEBRip'];
    codec_opts TEXT[] := ARRAY['x264','x265','HEVC','AVC','VP9','AV1'];
    audio_opts TEXT[] := ARRAY['AAC','AC3','DTS','DTS-HD.MA','TrueHD','FLAC','Opus','MP3','EAC3','Atmos'];
    group_opts TEXT[] := ARRAY['YIFY','RARBG','YTS','EVO','GALAXY','Tigole','PSA','QXR','Vyndros','Silence','MZABI','ION10','NTb','SPARKS','DDR','FGT','SPHD'];
    source_opts TEXT[] := ARRAY['crawler-de-01','crawler-de-02','crawler-sg-01','crawler-us-01','crawler-nl-01','crawler-jp-01'];
BEGIN
    RAISE NOTICE 'Generating 520 mock torrent records...';

    FOR i IN 1..520 LOOP
        -- Pick random category
        cat_idx := 1 + floor(random() * total_cats);
        CASE cat_idx
            WHEN 1 THEN base_name := movie_names[1 + floor(random() * array_length(movie_names,1))];
            WHEN 2 THEN base_name := tv_names[1 + floor(random() * array_length(tv_names,1))];
            WHEN 3 THEN base_name := music_names[1 + floor(random() * array_length(music_names,1))];
            WHEN 4 THEN base_name := software_names[1 + floor(random() * array_length(software_names,1))];
            WHEN 5 THEN base_name := book_names[1 + floor(random() * array_length(book_names,1))];
            WHEN 6 THEN base_name := misc_names[1 + floor(random() * array_length(misc_names,1))];
            WHEN 7 THEN base_name := anime_names[1 + floor(random() * array_length(anime_names,1))];
        END CASE;

        -- Build release name
        IF cat_idx IN (2, 7) THEN
            release_name := base_name || '.COMPLETE.'
                || quality_opts[1 + floor(random() * array_length(quality_opts,1))]
                || '.' || codec_opts[1 + floor(random() * array_length(codec_opts,1))]
                || '-' || group_opts[1 + floor(random() * array_length(group_opts,1))];
        ELSIF cat_idx = 3 THEN
            release_name := base_name || '.'
                || audio_opts[1 + floor(random() * array_length(audio_opts,1))]
                || '.24bit.96kHz-'
                || group_opts[1 + floor(random() * array_length(group_opts,1))];
        ELSIF cat_idx = 4 THEN
            release_name := base_name || '.x64-'
                || group_opts[1 + floor(random() * array_length(group_opts,1))];
        ELSE
            release_name := base_name || '.'
                || quality_opts[1 + floor(random() * array_length(quality_opts,1))]
                || '.' || codec_opts[1 + floor(random() * array_length(codec_opts,1))]
                || '-' || group_opts[1 + floor(random() * array_length(group_opts,1))];
        END IF;

        -- 40-char hex info_hash
        info_hash := left(md5(random()::text || i::text || clock_timestamp()::text) || md5(i::text || random()::text || clock_timestamp()::text), 40);

        -- Piece length
        piece_len := piece_lens[1 + floor(random() * array_length(piece_lens,1))];

        -- File count and total length by category
        IF cat_idx IN (2, 7) THEN
            file_cnt := 6 + floor(random() * 20);
            total_len := (1 + floor(random() * 5))::BIGINT * file_cnt * 1073741824;
        ELSIF cat_idx = 3 THEN
            file_cnt := 8 + floor(random() * 20);
            total_len := file_cnt * (5 + floor(random() * 35))::BIGINT * 1048576;
        ELSIF cat_idx = 4 THEN
            file_cnt := 1 + floor(random() * 5);
            total_len := (1 + floor(random() * 15))::BIGINT * 1073741824;
        ELSIF cat_idx = 5 THEN
            file_cnt := 1 + floor(random() * 3);
            total_len := (1 + floor(random() * 20))::BIGINT * 1048576;
        ELSIF cat_idx = 6 THEN
            file_cnt := 1 + floor(random() * 10);
            total_len := (1 + floor(random() * 4))::BIGINT * file_cnt * 1073741824;
        ELSE
            file_cnt := 1 + floor(random() * 3);
            total_len := (1 + floor(random() * 8))::BIGINT * 1073741824;
        END IF;

        created_age := make_interval(days => floor(random() * 90)::INT, hours => floor(random() * 24)::INT);

        INSERT INTO torrents (info_hash, name, piece_length, total_length, file_count, is_private, source, created_at, updated_at)
        VALUES (
            info_hash, release_name, piece_len, total_len, file_cnt,
            (random() < 0.02),
            source_opts[1 + floor(random() * array_length(source_opts,1))],
            NOW() - created_age,
            NOW() - created_age + make_interval(hours => floor(random() * 48)::INT)
        )
        RETURNING id INTO tid;

        -- Files
        FOR file_idx IN 1..file_cnt LOOP
            DECLARE
                fname TEXT;
                fext TEXT;
                flen BIGINT;
            BEGIN
                IF cat_idx IN (2, 7) THEN
                    fext := CASE floor(random() * 3) WHEN 0 THEN 'mkv' WHEN 1 THEN 'mp4' ELSE 'avi' END;
                    fname := base_name || '.E' || lpad(file_idx::TEXT, 2, '0') || '.' || fext;
                    flen := (1 + floor(random() * 5))::BIGINT * 1073741824;
                ELSIF cat_idx = 3 THEN
                    fext := CASE floor(random() * 4) WHEN 0 THEN 'flac' WHEN 1 THEN 'mp3' WHEN 2 THEN 'm4a' ELSE 'aac' END;
                    fname := lpad(file_idx::TEXT, 2, '0') || ' - Track ' || file_idx || '.' || fext;
                    flen := (5 + floor(random() * 35))::BIGINT * 1048576;
                ELSIF cat_idx = 4 THEN
                    IF file_cnt = 1 THEN
                        fname := base_name || '.iso';
                    ELSE
                        fext := CASE floor(random() * 3) WHEN 0 THEN 'rar' WHEN 1 THEN 'zip' ELSE '7z' END;
                        fname := base_name || '.part' || file_idx || '.' || fext;
                    END IF;
                    flen := total_len / file_cnt;
                ELSE
                    fext := CASE floor(random() * 4) WHEN 0 THEN 'mkv' WHEN 1 THEN 'mp4' WHEN 2 THEN 'avi' ELSE 'mov' END;
                    IF file_cnt = 1 THEN
                        fname := base_name || '.' || fext;
                    ELSE
                        fname := base_name || '.CD' || file_idx || '.' || fext;
                    END IF;
                    flen := total_len / file_cnt;
                END IF;

                INSERT INTO torrent_files (torrent_id, path_text, length) VALUES (tid, fname, flen);
            END;
        END LOOP;

    END LOOP;

    RAISE NOTICE 'Done. 520 torrents inserted.';
END $$;
