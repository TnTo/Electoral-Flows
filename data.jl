import CSV
import ExcelFiles
using DataFrames
using Query

mit = CSV.read("data/1976-2016-president.csv")
usep1 = DataFrame(ExcelFiles.load("data/1980-2014 November General Election.xlsx", "Turnout Rates", skipstartrows=1))
usep2 = DataFrame(ExcelFiles.load("data/2016 November General Election.xlsx", "Turnout Rates", skipstartrows=1))[1:52, :]
# usep3 = DataFrame(ExcelFiles.load("data/2018 November General Election.xlsx", "Turnout Rates", skipstartrows=1))[1:52, :]

for df in [usep1, usep2#=, usep3=#]
    rename!(df, (replace.(string.(names(df)), Ref(r"\s"=>""))))
    rename!(df, "Voting-EligiblePopulation(VEP)" => "VEP")
end

mit_clean = @from i in mit begin
    @where i.year >= 1980
    @select{i.year, state=i.state, i.candidate, i.party, i.candidatevotes}
    # @dropna(:candidate)
    @collect DataFrame
end

mit_total = @from i in mit begin
    @where i.year >= 1980
    @select{i.year, state=i.state, i.totalvotes}
    @collect DataFrame
end
unique!(mit_total)

usep1_clean = @from i in usep1 begin
    @where i.x1 != "United States" && i.Year % 4 == 0
    @select{year=i.Year, state=i.x1, i.VEP}
    @collect DataFrame
end

usep2_clean = @from i in usep2 begin
    @where i.x1 != "United States"
    @select{year=2016, state=i.x1, i.VEP}
    @collect DataFrame
end 

#=
usep3_clean = @from i in usep3 begin
    @where i.x1 != "United States"
    @select{state=i.x1, i.VEP}
    @collect DataFrame
end
=#

usep_clean = vcat(usep1_clean, usep2_clean)

df = outerjoin(usep_clean, mit_total, on=[:year, :state], makeunique=true)

CSV.write("data/candidates.csv", mit_clean)
CSV.write("data/totalvotes.csv", df)