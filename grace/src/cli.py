#!/usr/bin/env python3
"""Grace CLI - Command line interface for technical specification generation."""

import sys
import asyncio
import click
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import workflow modules
from .workflows import run_techspec_workflow
from .config import get_config


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Grace CLI - Generate technical specifications with Firecrawl and PDF support.

    Use 'grace techspec <connector>' to generate specs for a specific connector. 
    Use -e flag for Claude Agent SDK enhancement and field analysis, and -m to enable mock server generation for testing. 
    Use -f flag allows you to specify a folder with existing documentation.
    """
    pass

@cli.command()
@click.argument('connector', required=False)
@click.option('folder', '-f', help="Path to docs folder")
@click.option('urls', '-u', help="Path to URLs file")
@click.option('--output', '-o', help='Output directory for generated specs')
@click.option('--test-only', is_flag=True, help='Run in test mode without generating files')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--mock-server', '-m', is_flag=True, help='Enable mock server for testing')
@click.option('--enhance', '-e', is_flag=True, help='Enable Claude Agent SDK enhancement')
def techspec(connector, folder, urls, output, test_only, verbose, mock_server, enhance):
    """Generate technical specification for a connector.
    
    CONNECTOR: Name of the connector (e.g., gigadat)
    
    -u: Path to file containing URLs to scrape
    -f: Path to folder with existing documentation
    -e: Enable Claude Agent SDK enhancement and field analysis
    -m: Enable mock server generation
    """
    async def run_techspec():
        """Async wrapper for techspec workflow."""
        try:
            if verbose:
                click.echo(f"Starting techspec workflow...")
                click.echo(f"Connector: {connector}")
                if output:
                    click.echo(f"Output dir: {output}")
                if mock_server:
                    click.echo("Mock server: ENABLED")
                if enhance:
                    click.echo("Claude Agent Enhancement: ENABLED")
                if test_only:
                    click.echo("Mode: TEST ONLY")
                click.echo()

            if urls:
                click.echo(f"Docs URLs file: {urls}")
            # Use config for output directory if not specified
            config_instance = get_config()
            output_dir = output or config_instance.getTechSpecConfig().output_dir
            # Execute the techspec workflow
            result = await run_techspec_workflow(
                connector_name=connector,
                folder=folder,
                urls_file=urls,
                output_dir=output_dir,
                test_only=test_only,
                verbose=verbose,
                mock_server=mock_server,
                enhance=enhance,
            )

            if result["success"]:
                click.echo("Techspec generation completed successfully!")

                # Display output summary
                output_data = result.get("output", {})
                if output_data:
                    click.echo("\nGeneration Summary:")
                    click.echo(f"  • Connector: {output_data.get('connector_name', connector)}")

                    summary = output_data.get("summary", {})
                    if summary:
                        click.echo(f"  • Total files: {summary.get('total_files', 0)}")
                        click.echo(f"  • Code files: {summary.get('code_files', 0)}")
                        click.echo(f"  • Test files: {summary.get('test_files', 0)}")
                        click.echo(f"  • Documentation: {summary.get('documentation_files', 0)}")

                    output_dir_path = output_data.get("output_directory", f"./generated/{connector}")
                    if not test_only:
                        click.echo(f"  • Output directory: {output_dir_path}")

                        # Create output directory and files (in real implementation)
                        output_path = Path(output_dir_path)
                        output_path.mkdir(parents=True, exist_ok=True)

                        # Save a summary file
                        summary_file = output_path / "generation_summary.json"
                        import json
                        with open(summary_file, 'w') as f:
                            json.dump(result, f, indent=2, default=str)

                        click.echo(f"  • Summary saved: {summary_file}")

                    instructions = output_data.get("instructions", {})
                    if instructions:
                        click.echo("\nNext Steps:")
                        for step in instructions.get("next_steps", []):
                            click.echo(f"  • {step}")

                        if not test_only:
                            test_cmd = instructions.get("test_command")
                            build_cmd = instructions.get("build_command")
                            if test_cmd:
                                click.echo(f"\nTest command: {test_cmd}")
                            if build_cmd:
                                click.echo(f"Build command: {build_cmd}")

            else:
                # click.echo(f"result: {result}")
                if verbose and result.get("metadata"):
                    click.echo(f"Debug info: {result['metadata']}", err=True)
                sys.exit(1)

        except Exception as e:
            click.echo(f"Unexpected error: {str(e)}", err=True)
            if verbose:
                import traceback
                click.echo(f"Traceback: {traceback.format_exc()}", err=True)
            sys.exit(1)

    # Run the async workflow
    asyncio.run(run_techspec())

@cli.command()
@click.argument('connector')
@click.option('--flow', default='card', help='Payment flow to implement (e.g., card, bank_transfer, voucher)')
@click.option('--techspec', '-t', help='Path to existing techspec file')
@click.option('--branch', '-b', help='Git branch to work on')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
def integrate(connector, flow, techspec, branch, verbose):
    """Run full integration pipeline for a connector.
    
    This command orchestrates the complete integration workflow:
    1. Code generation (if not already done)
    2. Build the connector
    3. Test with grpcurl
    4. Create pull request
    
    CONNECTOR: Name of the connector (e.g., stripe, adyen)
    """
    async def run_integration():
        try:
            if verbose:
                click.echo(f"Starting integration workflow for {connector}...")
                click.echo(f"Flow: {flow}")
                if techspec:
                    click.echo(f"Techspec: {techspec}")
                click.echo()
            
            # Import the integration workflow
            from .workflows.integration_workflow import run_integration_workflow
            
            result = await run_integration_workflow(
                connector_name=connector,
                flow=flow,
                techspec_path=techspec,
                branch=branch,
                verbose=verbose
            )
            
            if result["success"]:
                click.echo(f"\nIntegration completed successfully!")
                if result.get("pr_url"):
                    click.echo(f"PR URL: {result['pr_url']}")
                
                # Output summary for Connector Forge to parse
                click.echo(f"\nCONNECTOR: {connector}")
                click.echo(f"FLOW: {flow}")
                click.echo(f"STATUS: SUCCESS")
                if result.get("pr_url"):
                    click.echo(f"PR_URL: {result['pr_url']}")
            else:
                click.echo(f"\nIntegration failed: {result.get('error', 'Unknown error')}")
                click.echo(f"\nCONNECTOR: {connector}")
                click.echo(f"FLOW: {flow}")
                click.echo(f"STATUS: FAILED")
                click.echo(f"REASON: {result.get('error', 'Unknown error')}")
                sys.exit(1)
                
        except Exception as e:
            click.echo(f"\nIntegration error: {str(e)}", err=True)
            if verbose:
                import traceback
                click.echo(f"Traceback: {traceback.format_exc()}", err=True)
            sys.exit(1)
    
    asyncio.run(run_integration())


def main():
    """Main entry point for Grace CLI."""
    try:
        cli()
    except KeyboardInterrupt:
        click.echo("\n\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        click.echo(f"\nError: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
