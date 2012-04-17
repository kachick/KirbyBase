require 'rubygems'
require 'kirbybase'
db = KirbyBase.new
db.drop_table(:stocks) if db.table_exists?(:stocks)

stocks_tbl = db.create_table(:stocks, :name, :String, :ticker, :String,
:tradeprice, :Float,:tradedate,:Date,:quantity,:Integer,:totalposition,{:DataType=>:Float,
:Calculated=>'quantity*tradeprice'})

stocks_tbl= db.get_table(:stocks)

stocks_tbl.insert do |r|
    r.name = 'Coca-Cola'
    r.ticker = 'KO'
    r.tradeprice = 72
    r.tradedate = Date.today
    r.quantity=100
end


stocks_tbl.insert do |r|
    r.name = 'Apple'
    r.ticker = 'AAPL'
    r.tradeprice = 580.13
    r.tradedate = Date.today
    r.quantity=10
end

stocks_tbl.insert do |r|
    r.name = 'IBM'
    r.ticker = 'IBM'
    r.tradeprice = 202.72
    r.tradedate = Date.today
    r.quantity=20
end

stocks_tbl.insert do |r|
    r.name = 'Amazon'
    r.ticker = 'AMZN'
    r.tradeprice = 185.50
    r.tradedate = Date.today
    r.quantity=45
end

results=stocks_tbl.select(:name,:ticker,:tradeprice,:tradedate,:quantity,:totalposition,:recno).sort(+:totalposition).to_report
puts results 

