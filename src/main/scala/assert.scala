val ns = List(1, 2, 3, 4)
val s0 = ns.foldLeft (0) (_+_) //10
val s1 = ns.fold (1) (_+_) //10
assert(s0 == s1) //AssertionError: assertion failed
//Adding new line for branching testing
