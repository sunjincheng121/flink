/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;

import static org.apache.flink.client.cli.CliFrontendParser.CLASS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;

/**
 * Command line options for the Python RUN command.
 */
public class RunPyOptions extends ProgramOptions {

	private final String[] pythonProgramArgs;

	public RunPyOptions(CommandLine line) throws CliArgsException {
		super(line);

		String[] args = getProgramArgs();

		// If specified the option -py(--python)
		if (line.hasOption(PY_OPTION.getOpt())) {
			// Cannot use option -py and -pym simultaneously.
			if (line.hasOption(PYMODULE_OPTION.getOpt())) {
				throw new CliArgsException("Cannot use option -py and -pym simultaneously.");
			}
			// The cli cmd args which will be transferred to PythonDriver will be transformed as follows:
			// CLI cmd : -py ${python.py} pyfs [optional] ${py-files} [optional] ${other args}.
			// PythonDriver args: py ${python.py} [optional] pyfs [optional] ${py-files} [optional] ${other args}.
			// -------------------------------transformed-------------------------------------------------------
			// e.g. -py wordcount.py(CLI cmd) -----------> py wordcount.py(PythonDriver args)
			// e.g. -py wordcount.py -pyfs file:///AAA.py,hdfs:///BBB.py --input in.txt --output out.txt(CLI cmd)
			// 	-----> -py wordcount.py -pyfs file:///AAA.py,hdfs:///BBB.py --input in.txt --output out.txt(PythonDriver args)
			String[] newArgs;
			int argIndex;
			if (line.hasOption(PYFILES_OPTION.getOpt())) {
				newArgs = new String[args.length + 4];
				newArgs[2] = "-" + PYFILES_OPTION.getOpt();
				newArgs[3] = line.getOptionValue(PYFILES_OPTION.getOpt());
				argIndex = 4;
			} else {
				newArgs = new String[args.length + 2];
				argIndex = 2;
			}
			newArgs[0] = "-" + PY_OPTION.getOpt();
			newArgs[1] = line.getOptionValue(PY_OPTION.getOpt());
			System.arraycopy(args, 0, newArgs, argIndex, args.length);
			args = newArgs;
		}

		// If specified the option -pym(--pyModule)
		if (line.hasOption(PYMODULE_OPTION.getOpt())) {
			// If you specify the option -pym, you should specify the option --pyFiles simultaneously.
			if (!line.hasOption(PYFILES_OPTION.getOpt())) {
				throw new CliArgsException("-pym must be used in conjunction with `--pyFiles`");
			}
			// The cli cmd args which will be transferred to PythonDriver will be transformed as follows:
			// CLI cmd : -pym ${py-module} -pyfs ${py-files} [optional] ${other args}.
			// PythonDriver args: -pym ${py-module} -pyfs ${py-files} [optional] ${other args}.
			// e.g. -pym AAA.fun -pyfs AAA.zip(CLI cmd) ----> -pym AAA.fun -pyfs AAA.zip(PythonDriver args)
			String[] newArgs = new String[args.length + 4];
			newArgs[0] = "-" + PYMODULE_OPTION.getOpt();
			newArgs[1] = line.getOptionValue(PYMODULE_OPTION.getOpt());
			newArgs[2] = "-" + PYFILES_OPTION.getOpt();
			newArgs[3] = line.getOptionValue(PYFILES_OPTION.getOpt());
			System.arraycopy(args, 0, newArgs, 4, args.length);
			args = newArgs;
		}

		this.pythonProgramArgs = args;
	}

	@Override
	public String getEntryPointClassName() {
		String entryPointClass = super.getEntryPointClassName();

		// If the job is Python Shell job, the entry point class name is PythonGateWayServer.
		// Otherwise, the entry point class of python job is PythonDriver
		if (entryPointClass == null) {
			entryPointClass = "org.apache.flink.client.python.PythonDriver";
		}
		return entryPointClass;
	}

	@Override
	public String[] getProgramArgs() {
		return this.pythonProgramArgs;
	}

	@Override
	public boolean isPython() {
		return true;
	}

	public static boolean isPython(CommandLine line) {
		String entryPointClass = line.hasOption(CLASS_OPTION.getOpt()) ?
			line.getOptionValue(CLASS_OPTION.getOpt()) : null;

		return line.hasOption(PY_OPTION.getOpt()) | line.hasOption(PYMODULE_OPTION.getOpt())
			| "org.apache.flink.client.python.PythonGatewayServer".equals(entryPointClass);
	}

	/**
	 * Gets the JAR file from the path.
	 *
	 * @param jarFilePath The path of JAR file
	 * @return The JAR file
	 * @throws FileNotFoundException The JAR file does not exist.
	 */
	private File getJarFile(String jarFilePath) throws FileNotFoundException {
		File jarFile = new File(jarFilePath);
		// Check if JAR file exists
		if (!jarFile.exists()) {
			throw new FileNotFoundException("JAR file does not exist: " + jarFile);
		}
		else if (!jarFile.isFile()) {
			throw new FileNotFoundException("JAR file is not a file: " + jarFile);
		}
		return jarFile;
	}
}
