/*
 *    _____      _ ____  ____
 *   / ___/_____(_) __ \/ __ )
 *   \__ \/ ___/ / / / / __  |
 *  ___/ / /__/ / /_/ / /_/ / 
 * /____/\___/_/_____/_____/  
 *
 *
 * BEGIN_COPYRIGHT
 *
 * This file is part of SciDB.
 * Copyright (C) 2008-2014 SciDB, Inc.
 *
 * SciDB is free software: you can redistribute it and/or modify
 * it under the terms of the AFFERO GNU General Public License as published by
 * the Free Software Foundation.
 *
 * SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
 * INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
 * NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
 * the AFFERO GNU General Public License for the complete license terms.
 *
 * You should have received a copy of the AFFERO GNU General Public License
 * along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 * END_COPYRIGHT
 */
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>

#include "query/Operator.h"
#include "system/Exceptions.h"
#include "query/LogicalExpression.h"

#include <log4cxx/logger.h>

using namespace std;
using namespace scidb;

namespace orderbook
{

// Logger (static to prevent visibility of variable outside of file)
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.ops.orderbook"));

struct Order
{
  enum Type
  {
    NONE = 0,
    BUY = 1,
    SELL = 2
  };

  Type type;
  int32_t price; //in cents
  uint32_t volume;
  
  Order():
     type(NONE), price(0), volume(0)
  {}

  void reset()
  {
     type = NONE; 
     price = 0;
     volume = 0;
  }
}; 


class orderbookAggregate : public Aggregate
{
private:
    map<uint64_t, Order>    _idToOrder;
    map<int, int>         _priceToBuyVolume;
    map<int, int>         _priceToSellVolume;
    string                _currentSymbol;
    bool                  _flag;
    map<int, int>::reverse_iterator _highestBuyIter;
    map<int, int>::iterator         _lowestSellIter;

public:
    orderbookAggregate(const string& name, Type const& aggregateType):
        Aggregate(name, aggregateType, aggregateType),
        _flag(true)
    {
    }

    virtual AggregatePtr clone() const
    {
        return AggregatePtr(new orderbookAggregate(getName(), getAggregateType()));
    }

    AggregatePtr clone(Type const& aggregateType) const
    {
        return AggregatePtr(new orderbookAggregate(getName(), aggregateType));
    }

    bool ignoreNulls() const
    {
        return true;
    }

    Type getStateType() const
    {
        return getAggregateType();
    }

    void initializeState(Value& state)
    {
        state.setNull();
    }
    
    void innerIncorporateOrder(Order const& order, uint64_t const id, char action)
    {
        switch (action)
        {
        case 'A':   // Add an order
        {
           pair<map<uint64_t,Order>::iterator, bool> p = _idToOrder.insert(pair<uint64_t,Order>(id, order));
           if (p.second)
           {
               if (order.type == Order::BUY)
               {
                   int& volume = _priceToBuyVolume[order.price];
                   volume += order.volume;
                   if (_highestBuyIter == _priceToBuyVolume.rend() || _highestBuyIter->first < order.price )
                   {
                       _highestBuyIter = _priceToBuyVolume.rbegin();
                   }
               }
               else
               {
                   int& volume = _priceToSellVolume[order.price];
                   volume += order.volume; 
                   if (_lowestSellIter == _priceToSellVolume.end() || _lowestSellIter->first > order.price )
                   {
                      _lowestSellIter = _priceToSellVolume.begin();
                   }
               }
           }
           break;
        }
        case 'M':   // Modify an order
        {
          map<uint64_t, Order>::iterator orderIter = _idToOrder.find(id);
          if ( orderIter == _idToOrder.end())
          {
             break;
          }
          Order::Type type = orderIter->second.type;
          int32_t oldPrice = orderIter->second.price;
          int32_t oldVolume = orderIter->second.volume;
          if (type == Order::BUY)
          {
               map<int,int>::iterator iter = _priceToBuyVolume.find(oldPrice);
               iter->second -= oldVolume;
               if (iter->second <= 0)
                    _priceToBuyVolume.erase(iter);
          }
          else
          {
               map<int,int>::iterator iter = _priceToSellVolume.find(oldPrice);
               iter->second -= oldVolume;
               if (iter->second <= 0)
                    _priceToSellVolume.erase(iter);
          }
          orderIter->second.type = order.type;
	  orderIter->second.price = order.price;
          orderIter->second.volume = order.volume;
          if (order.type == Order::BUY)
          {
              int& volume = _priceToBuyVolume[order.price];
              volume += order.volume;
          }
          else
          {
              int& volume = _priceToSellVolume[order.price];
              volume += order.volume;
          }
          _highestBuyIter = _priceToBuyVolume.rbegin();
          _lowestSellIter = _priceToSellVolume.begin();
          break;
        }
        case 'D':   // Delete an order
        {
          map<uint64_t, Order>::iterator orderIter = _idToOrder.find(id);
          if ( orderIter == _idToOrder.end())
          {
             break;
          }
          Order::Type type = orderIter->second.type;
          int32_t price = orderIter->second.price;
          int32_t volume = orderIter->second.volume;
          if (type == Order::BUY)
          {
               map<int,int>::iterator iter = _priceToBuyVolume.find(price);
               iter->second -= volume;
               if (iter->second <= 0)
                    _priceToBuyVolume.erase(iter);
               _highestBuyIter = _priceToBuyVolume.rbegin();
          }
          else
          {
               map<int,int>::iterator iter = _priceToSellVolume.find(price);
               iter->second -= volume;
               if (iter->second <= 0)
                    _priceToSellVolume.erase(iter); 
               _lowestSellIter = _priceToSellVolume.begin();
          }
          _idToOrder.erase(orderIter);  
          break;
        }
        default:
          break;
        }
    }

    void incorporateOrder (string const& orderString)
    {
        stringstream ss(orderString);
        Order order;
        string orderField;
        uint64_t id = 0;
        int counter = 0;
        char action;
        while (getline(ss, orderField, ','))
        {
          if (counter == 0)
          {
              action = orderField[0];
              if (action != 'A' && action != 'M' && action != 'D')
              {
                  ostringstream err; 
                  err<<"Encoutnered illegal action; action="<<action<<" orderField="<< orderField<< "orderString="<<orderString;
                  throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << err.str();
              }
          }
          else if (counter == 1)
          { 
             id = strtoull(orderField.c_str(), NULL, 10);
          }
          else if (counter == 2)
          {
             order.price = int32_t(1000*atof(orderField.c_str()));
             if (order.price < 0)
                  throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered negative price";
          }
          else if (counter == 3)
          {
             order.volume = atoi(orderField.c_str());
          }
          else if (counter == 4) 
          {  //symbol
          }
          else if (counter == 5)
          {
             if (orderField[0] == 'B')  
                 order.type = Order::BUY;
             else if (orderField[0] == 'S')
                 order.type = Order::SELL;
             else                             
                 throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered illegal order type";
          } 
          else
          {
             throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered an order with more than 6 fields";
          }
          ++counter;
        }
        innerIncorporateOrder(order, id, action);
    }   

    string getSymbol(string const& val)
    {
        stringstream ss(val);
        string symbol;
        int counter = 0;
        while (getline(ss, symbol, ','))
        {    
           if (counter == 4)
               return symbol;
           ++counter;
        }
        return symbol;
    }

    void accumulate(Value& state, Value const& input)
    {
      string val = input.getString();
      string symbol = getSymbol(val);
      if(symbol!=_currentSymbol)
      {
          _priceToSellVolume.clear();
          _priceToBuyVolume.clear();
          _idToOrder.clear();
          _currentSymbol = symbol;
          _flag = true;
          _highestBuyIter = _priceToBuyVolume.rend();
          _lowestSellIter = _priceToSellVolume.end();
      }
      if (_flag == true)
      {
          _flag = false;
      }
      else
      {
          _flag =true;
          return;
      }
      stringstream ss(val);
      string item;
      vector<string> adds;
      vector<string> mods;
      vector<string> dels;
      while (getline(ss, item, '|'))
      {
          if (item[0]=='A')
              adds.push_back(item);
          else if (item[0] =='M')
              mods.push_back(item);
          else if (item[0] == 'D' )
              dels.push_back(item);
          else
              throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Encountered a record with improper starting field";
      }
      for (size_t i =0; i<adds.size(); ++i)
      {
          incorporateOrder(adds[i]);
      }
      for (size_t i =0; i<mods.size(); ++i)
      {
          incorporateOrder(mods[i]);
      }
      for (size_t i =0; i<dels.size(); ++i)
      {
          incorporateOrder(dels[i]);
      }
    }

    void merge(Value& dstState, Value const& srcState)
    {
      throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "All time must be in one chunk!";
    }

    void finalResult(Value& result, Value const& state)
    {
        char buf[512]; 
/* Extra book depth of 3, super cheesy... */
        Order hb1,hb2,hb3;
        hb1.price = -1; hb2.price = -1; hb3.price = -1;
        if (_highestBuyIter != _priceToBuyVolume.rend())
        {
            hb1.price = _highestBuyIter->first;
            hb1.volume = _highestBuyIter->second;
        ++_highestBuyIter;
        if(_highestBuyIter != _priceToBuyVolume.rend())
        {
            hb2.price = _highestBuyIter->first;
            hb2.volume = _highestBuyIter->second;
        ++_highestBuyIter;
        if(_highestBuyIter != _priceToBuyVolume.rend())
        {
            hb3.price = _highestBuyIter->first;
            hb3.volume = _highestBuyIter->second;
        }
        }
        }

        Order ls1,ls2,ls3;
        ls1.price = -1; ls2.price = -1; ls3.price = -1;
        if (_lowestSellIter != _priceToSellVolume.end())
        {
            ls1.price = _lowestSellIter->first;
            ls1.volume = _lowestSellIter->second;
        ++_lowestSellIter;
        if (_lowestSellIter != _priceToSellVolume.end())
        {
            ls2.price = _lowestSellIter->first;
            ls2.volume = _lowestSellIter->second;
        ++_lowestSellIter;
        if (_lowestSellIter != _priceToSellVolume.end())
        {
            ls3.price = _lowestSellIter->first;
            ls3.volume = _lowestSellIter->second;
         }
         }
         }

        double p1,p2,p3,v1,v2,v3;
        double p4,p5,p6,v4,v5,v6;
        p1 = p2 = p3 = v1 = v2 = v3 = NAN;
        p4 = p5 = p6 = v4 = v5 = v6 = NAN;
        if(hb1.price>=0) {p1 = hb1.price/1000.0; v1 = hb1.volume;}
        if(hb2.price>=0) {p2 = hb2.price/1000.0; v2 = hb2.volume;}
        if(hb3.price>=0) {p3 = hb3.price/1000.0; v3 = hb3.volume;}
        if(ls1.price>=0) {p4 = ls1.price/1000.0; v4 = ls1.volume;}
        if(ls2.price>=0) {p5 = ls2.price/1000.0; v5 = ls2.volume;}
        if(ls3.price>=0) {p6 = ls3.price/1000.0; v6 = ls3.volume;}
        snprintf(buf, 512, "%.3f,%.0f,%.3f,%.0f,%.3f,%.0f,%.3f,%.0f,%.3f,%.0f,%.3f,%.0f",
                 p3, v3, p2, v2, p1, v1, p4, v4, p5, v5, p6, v6);
        result.setString(buf);
    }

};



vector<AggregatePtr> _aggregates;
EXPORTED_FUNCTION const vector<AggregatePtr>& GetAggregates()
{
    return _aggregates;
}

class orderbookAggregateGeneratorInstance
{
public:
    orderbookAggregateGeneratorInstance()
    {
        //Add new aggregates here:
        _aggregates.push_back(AggregatePtr(new orderbookAggregate("orderbook", TypeLibrary::getType(TID_VOID))));

    }
} _aggregateGeneratorInstance;

}
