/*
NoSQL Assignment
*/

use "coffee"

// Q1
// Collection considered: <baristacoffeesalesTBL> 
// How many product categories are there? For each product category, show the number of records.
db.baristacoffeesalestbl.aggregate([
    {
        $group:{_id:"$product_category",records:{$sum:1}}
    }
]);

// Q2
// Collectionconsidered: <caffeine_intake_tracker>
// What is the average caffeine per beverage type (coffee/tea/energy drink)? Hint: $switch
db.caffeine_intake_tracker.aggregate([
 {
    $project: {
        beverage:{
            $switch: {
              branches: [
                 { case: {$eq:["$beverage_coffee","True"]}, then: "coffee" },
                 { case: {$eq:["$beverage_energy_drink","True"]}, then: "energy_drink" },
                 { case: {$eq:["$beverage_tea","True"]}, then: "tea" }
              ],
              default: "none"
            } 
        },
        caffeine_mg:1
    }
 },
 {$match:{beverage:{$ne:"none"}}},
 {$group:{_id:"$beverage",avg_caffeine:{$avg: "$caffeine_mg"},count:{$sum:1}}},
 {$sort:{avg_caffine: -1}}
]);

// Q3
// Collection considered: <caffeine_intake_tracker> 
// How does sleep impact rate vary by time of day (morning/afternoon/evening)? Hint: $switch
db.caffeine_intake_tracker.aggregate([
 {
    $project: {
        time_of_day:{
            $switch: {
              branches: [
                 { case: {$eq:["$time_of_day_afternoon","True"]}, then: "afternoon" },
                 { case: {$eq:["$time_of_day_evening","True"]}, then: "evening" },
                 { case: {$eq:["$time_of_day_morning","True"]}, then: "morning" }
              ],
              default: "unknown"
            } 
        },
        sleep_impacted: 1
    }
 }, 
 {$match: {time_of_day: {$ne:"unknown"}}},
 {
     $group: { _id: "$time_of_day",
         impacted_rate: {$avg: {$toDouble:"$sleep_impacted"}},
         n:{$sum: 1}
     }
 },
 {$sort:{impacted_rate:-1}}
]);

// Q4
// Collection considered: <caffeine_intake_tracker> 
// Bucket caffeine into Low/Med/High and compare average sleep quality. Hint: $bucket, $addFields, $switch
db.caffeine_intake_tracker.aggregate([
{
    $bucket: {
          groupBy: "$caffeine_mg",
          boundaries: [ 0, 0.25, 0.5, 1.01 ],
          default: "unknown",
          output: {
            avg_sleep_quality:{$avg:"$sleep_quality"},
            avg_focus:{$avg: "$focus_level"},
            n:{$sum:1}
          }
        }
    },
        {
            $addFields: {
                caffeine_band:{
                $switch: {
                  branches: [
                     { case: {$eq:["$_id",0]}, then: "low" },
                     { case: {$eq:["$_id",0.25]}, then: "med" },
                     { case: {$eq:["$_id",0.5]}, then: "high" },
                  ],
                  default: "unknown"
                }
            }
        }
},
{$project: {_id:0}},
{$sort: {caffine_band:1}}
]);

// Q5
// Collection considered: <coffeesales> 
// What is the total revenue and order count? Hint: $addFields
db.coffeesales.aggregate([
    {
        $addFields: {money_num:{$toDouble: "$money"}}
    },
    {$group:{_id:null,orders:{$sum:1},revenue:{$sum:"$money_num"}}},
    {$project:{_id:0}}
]);

// Q6
// Collection considered: <coffeesales>
// Which drink is most cash-heavy? (cash share by drink)
db.coffeesales.aggregate([
    {
    $addFields: {is_cash:{$eq:["$cash_type","cash"]},
    money_num:{$toDouble: "$money"}}   
    },
    {
        $group: { _id: "$coffee_name",
            cash_orders:{$sum:{$cond:["$is_cash",1,0]}},
            total_orders:{$sum:1},
            cash_rev:{$sum: {$cond:["$is_cash","$money_num",0]}},
            total_rev:{$sum:"$money_num"}
        }},
        {
            $project:{
                coffee_name:"$_id",
                cash_order_share:{$cond:[{$gt:["$total_orders",0]},{$divide:["$cash_orders","$total_orders"]},null]
            },
            cash_revenue_share:{$cond:[{$gt:["$total_rev",0]},{$divide:["$cash_rev","$total_rev"]},null]
        },
        _id:0
    }},
    {$sort:{cash_revenue_share: -1}}
]);