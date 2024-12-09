# Standard library imports
import os
from datetime import datetime

# Third-party library imports
from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import Table, SimpleDocTemplate, Image, Paragraph, Spacer

# Dagster imports
from dagster import asset, AssetExecutionContext, Config

# Local module imports
from . import constants



class ReportConfig(Config):
    """
    A configuration class for handling the report generation config 
    coming from the sensor in Dagster.

    Attributes:
        filename (str): The name of the JSON file containing post summaries.
        summaries (list): A list of summaries loaded from the JSON file, 
                          where each summary is represented as a Python dictionary. 
    """
    
    filename: str
    summaries: list


@asset(
    deps=['plot_weekly_post_counts', 'summarize_robot_posts'],
    group_name='reporting',
)
def generate_pdf_reports(context: AssetExecutionContext, config: ReportConfig) -> None:
    """
    Generates a PDF report with a title, date, plot, and summaries of robot-related posts.
    """
    
    # Extract the base filename (without extension) from the config
    filename_no_ext = os.path.splitext(config.filename)[0]
    
    # Generate output path for the PDF report and path to the plot image
    output_path = constants.REPORTS_TEMPLATE_FILE_PATH.format(filename_no_ext)
    plot_path= constants.WEEKLY_PLOT_FILE_PATH
    
    # Initialize the PDF document template
    pdf = SimpleDocTemplate(output_path, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Add a title to the PDF
    title = Paragraph("Reddit Humanoid Report", styles['Title'])
    elements.append(title)
    
    # Define a custom style for the centered date
    centered_style = ParagraphStyle(
    name="Centered",
    alignment=1,  # 1 means center alignment
    fontSize=12,
    textColor=colors.black,
    )
    
    # Extract and format the date from the filename and add it below the title
    date_str = datetime.strptime(filename_no_ext.split("_")[-1], "%Y-%m-%d").strftime("%B %d, %Y")
    date = Paragraph(date_str, centered_style)
    elements.append(date)
    elements.append(Spacer(1, 0.4 * inch))

    # Add a heading for the Plot section
    elements.append(Paragraph("Weekly Post Plot:", styles['Heading2']))
    elements.append(Spacer(1, 0.25 * inch))
    
    # Add the plot to the PDF
    try:
        with PILImage.open(plot_path) as img:
            # Get the original width and height of the image
            width, height = img.size
            aspect_ratio = height / width
            
            # Scale the image to fit within 6 inches, maintaining aspect ratio
            img_width = 6 * inch
            img_height = img_width * aspect_ratio
            
            # Add the image to the PDF
            elements.append(Image(plot_path, width=img_width, height=img_height))
            elements.append(Spacer(1, 0.4 * inch))
    except FileNotFoundError:
        elements.append(Paragraph("Error: Plot image not found.", styles['Normal']))
        elements.append(Spacer(1, 0.4 * inch))
    
    # Add a heading for the summaries section
    elements.append(Paragraph("Post Summaries:", styles['Heading2']))
    elements.append(Spacer(1, 0.25 * inch))
    
    # Add each summary to the PDF
    for idx, summary in enumerate(config.summaries):
        # Extract fields from the summary, providing defaults if keys are missing
        post_id = summary.get("post_id", "N/A")
        post_permalink = summary.get("post_permalink", "N/A")
        humanoid = summary.get("humanoid", "N/A")
        title = summary.get("title", "N/A")
        summary_text = summary.get("summary", "N/A")
        themes = summary.get("themes", [])
        n_comments = summary.get("n_comments", "N/A")

         # Format themes as a bulleted list with line breaks
        themes_formatted = "<br/>".join([f"    • {theme}" for theme in themes])
        
        # Create the paragraph content for this summary
        summary_paragraph = f"""
        Summary {idx+1} of {len(config.summaries)} — {humanoid} <br/> <br/>
        <b>Title:</b> {title} <br/> <br/>
        <b>Summary:</b> <br/> <br/> {summary_text} <br/> <br/>
        <b>Themes:</b> <br/> <br/> {themes_formatted} <br/> <br/>
        <b>Post ID:</b> {post_id} <br/>
        <b>Total Comments:</b> {n_comments} <br/>
        <b>Post Permalink:</b> <a href="{post_permalink}">{post_permalink}</a> <br/> <br/>
        """
        
        # Add the summary paragraph to the PDF
        elements.append(Paragraph(summary_paragraph, styles['Normal']))
        elements.append(Spacer(1, 0.15 * inch))
        
        # Add a line separator between summaries
        elements.append(Table([[Spacer(1, 1)]], style=[("LINEBELOW", (0, 0), (-1, -1), 0.5, colors.black)]))  # Line separator
        elements.append(Spacer(1, 0.15 * inch))
        
    # Build the PDF
    pdf.build(elements)
    context.log.info(f"PDF report successfully generated at: {output_path}")