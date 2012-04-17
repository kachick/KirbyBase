require 'camping'
require 'camping-kirbybase'

Camping.goes :CampingStocks

module CampingStocks

  module Models 
  end   
    
  module Controllers
      
    class Index < R '/'
      def get
        render :index 
      end
    end
  
    class Page < R '/(\w+)'
      def get(page_name)
        render page_name
      end
    end
  end 
  
  module Views    
    def layout
      self << yield
    end
    
    def index
      kirby = KirbyBase.new
      stocks_tbl = kirby.get_table(:stocks)
      results = stocks_tbl.select(:name,:ticker,:tradeprice,:tradedate,:quantity,:totalposition,:recno).sort(+:totalposition)

        table do
        tr do
         td 'Stock Name'
         td 'Ticker'
         td 'Number of Stocks'
         td 'Price'
         td 'Date'
         td 'Total Position'
       end
       for result in results
         tr do
           td result.name
           td result.ticker
           td result.quantity
           td result.tradeprice
           td result.tradedate
           td result.totalposition
         end
       end
      end
    end    
  end    
end