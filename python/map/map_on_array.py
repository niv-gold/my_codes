
def manipulate_arr(arr:list):   
    lst_1 = [[pow(i,2) for i in lst if i>0] for lst in arr ]
    print(lst_1)
    return lst_1

def multiply_arr_map(arr:list):
    print(arr)
    pos_num = lambda x: x>0
    square = lambda x: x**2
    res = [list(map(square,filter(pos_num,sub_lst))) for sub_lst in arr]
    print(res)
    return res

def main():
    try:
        lst = [[-7,0,-9],[111],[-1,-8,0,10,3,7],[12]]
        # output = manipulate_arr(lst)
        output = multiply_arr_map(lst)
    except Exception as e:
        print(e)

# run the code
main()

