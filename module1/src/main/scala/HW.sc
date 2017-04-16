val text ="Hello world"

def sayHello(name : String) ={
  s"hello $name!"
}

var slow = sayHello("Ragu1")

def sayHello1(name : String)(whoAreYou : () => String) ={
  s"hello $name!, my name is ${whoAreYou}"
}

def provideName() : String = {
  s"Scala"
}

var fast = sayHello1("fast")(provideName)

var faster = sayHello1("faster")(() => {
  "faster"
})