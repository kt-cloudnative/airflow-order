from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG666B',WORKFLOW_ID='8760b83822f24e2c94c87a1998b7cced', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG666B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 6, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8760b83822f24e2c94c87a1998b7cced')

    csng666bJob_vol = []
    csng666bJob_volMnt = []
    csng666bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng666bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng666bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng666bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng666bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng666bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd38b832c91c246f49a5f92fd2000ba68',
        'volumes': csng666bJob_vol,
        'volume_mounts': csng666bJob_volMnt,
        'env_from':csng666bJob_env,
        'task_id':'csng666bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.55',
        'arguments':["--job.name=csng666bJob", "fromDate=20240731", "toDate=20240731", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8760b83822f24e2c94c87a1998b7cced')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng666bJob,
        Complete
    ]) 

    # authCheck >> csng666bJob >> Complete
    workflow








