import org.broadinstitute.sting.queue.QScript


/*

## Docs

- Base: <http://www.broadinstitute.org/gatk/guide/topic?name=queue>
- QScripts: <http://www.broadinstitute.org/gatk/guide/article?id=1307>

## Relevant sources

Source of the framework:

    ./public/queue-framework/src/main/scala/org/broadinstitute/sting/queue/QScript.scala
    ./public/queue-framework/src/main/scala/org/broadinstitute/sting/queue/function/CommandLineFunction.scala
    ./public/queue-framework/src/main/scala/org/broadinstitute/sting/queue/function/QFunction.scala

Real-world examples:

    ./public/queue-framework/src/main/qscripts/org/broadinstitute/sting/queue/qscripts/examples/ExampleUnifiedGenotyper.scala

## Running

Dry-run:

    java -jar target/Queue.jar -S Seb_tests/qscript_seb.scala --scriptinput README.md

Wet-run:

    java -jar target/Queue.jar -S Seb_tests/qscript_seb.scala --scriptinput README.md -run


## Run Engines

The `qsub` one:

    ./public/queue-framework/src/main/scala/org/broadinstitute/sting/queue/engine/gridengine/GridEngineJobRunner.scala

One needs to implemement `start`, `status`, `init`, and `tryStop`.

*/



// Defining one QScript
class MyScript extends QScript {

  // Arguments to add to the command line

  // Everything must be a java.io.File
  @Input(doc="Initial QScript input file")
  var scriptInput: File = _  // '_' is null
  // `scriptInput` becomes `--scriptinput <file>`


  // Combinators to build command lines, exploiting `required`, `optional`, etc
  // from `CommandLineFunction`
  // They are described there: 
  //   http://gatkforums.broadinstitute.org/discussion/1312/queue-commandlinefunctions
  abstract class ABitBetterCommandLineFunction extends CommandLineFunction {
    val appendRedirect = required(">>", escape=false)
    val chainWith = required("&&", escape=false)
    val cat = required("cat")
    val group = (command: String) =>
        required("(", escape=false) + command + required(")", escape=false)
    val date = required("date") + required("-R")
    def groupChain(commands: List[String]) : String = commands match {
        case Nil => ""
        case one :: Nil => group(one)
        case one :: two :: more =>
            group(one) + chainWith + groupChain(two :: more)
    }
    def echo(s: String) = required("echo") + required(s)
  }


  // This is the actual “script” of the QScript
  def script = {

    // Some Debug info:
    var creationDate = new java.util.Date()

    // A command that copies the `scriptInput` argument into some file:
    val firstCommand = new ABitBetterCommandLineFunction {
      @Input(doc="Input of First Command") var inputFile : File = _
      @Output(doc="Output of First Command") var outputFile : File = _
      def commandLine = cat + required(inputFile) + appendRedirect + outputFile
    }
    firstCommand.inputFile = scriptInput
    firstCommand.outputFile = new File("./default_output_of_first_command")
    add(firstCommand) 
    // `add` just adds to a list, can take more arguments → add as a list

    // The second command takes the output of the first one, copies it, and
    // adds the date (at exec time):
    val secondCommand = new ABitBetterCommandLineFunction {
      @Input(doc="Input of second Command") var inputFile : File = _
      @Output(doc="Output of second Command") var outputFile : File = _
      def commandLine = 
          group (required("sleep") + 10) + chainWith +
          group (cat + required(inputFile) + appendRedirect + outputFile) + 
          chainWith + group (date + appendRedirect + outputFile)
    }
    secondCommand.inputFile = firstCommand.outputFile
    secondCommand.outputFile = new File("./default_output_of_second_command")
    add(secondCommand)

    // The third command depends on the first and second
    val thirdCommand = new ABitBetterCommandLineFunction {
      @Input(doc="Input 1 of third Command") var inputFile1 : File = null
      @Input(doc="Input 2 of third Command") var inputFile2 : File = null
      @Output(doc="Output of third Command") var outputFile : File = _
      def commandLine = groupChain(List(
          echo("============  Third Command (creation: " + creationDate + 
              ") (evaluated: " + new java.util.Date() + ")") + appendRedirect + outputFile,
          echo("   =========  input 1") + appendRedirect + outputFile,
          cat + required(inputFile1) + appendRedirect + outputFile,
          echo("   =========  input 2") + appendRedirect + outputFile,
          cat + required(inputFile2) + appendRedirect + outputFile,
          echo("   =========  Exec date:") + appendRedirect + outputFile,
          date + appendRedirect + outputFile))
    }
    thirdCommand.inputFile1 = firstCommand.outputFile
    thirdCommand.inputFile2 = secondCommand.outputFile
    thirdCommand.outputFile = new File("./default_output_of_third_command")
    add(thirdCommand)

  }

}
