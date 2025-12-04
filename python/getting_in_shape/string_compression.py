# declare variables
chars = ["a","a","a","a","a","b"]

# functions
class Solution:
    
    def compress(self, chars: list[str])-> int:
        s = ''
        P_upd = 0
        P_char = 0
        chars_len = len(chars)

        while P_char < chars_len:
            cnt=1

            while ((P_char+cnt)<chars_len) and (chars[P_char]==chars[P_char+cnt]):
                cnt+=1

            if cnt>1:
                s = chars[P_char]+str(cnt)
                for i in range(0,len(s)):
                    chars[P_upd]=s[i]
                    P_upd+=1          
``            else: 
                    chars[P_upd]=chars[P_char]
                    P_upd+=1
            
            P_char=P_char+cnt

        chars = chars[0:P_upd]
        print(chars)
        return len(chars)
        

# -----------------------------------------------------------------------------
# run class method
# -----------------------------------------------------------------------------
inst = Solution()
print(inst.compress(chars))