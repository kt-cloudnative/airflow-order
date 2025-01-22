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
                , WORKFLOW_NAME='ET_CSNG466B',WORKFLOW_ID='0128d14dcecb49189f5094c4fee85d76', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNG466B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0128d14dcecb49189f5094c4fee85d76')

    csng466bJob_vol = []
    csng466bJob_volMnt = []
    csng466bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng466bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng466bJob_env = [getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISConfigMap('icis-oder-etcterr-batch-configmap2'), getICISSecret('icis-oder-etcterr-batch-secret')]
    csng466bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng466bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng466bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cafee285733446c086f921ecc5cdbc1f',
        'volumes': csng466bJob_vol,
        'volume_mounts': csng466bJob_volMnt,
        'env_from':csng466bJob_env,
        'task_id':'csng466bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.1',
        'arguments':["--job.name=csng466bJob", "procDate=${YYYYMMDD}", "regOfcCd=999999", "regEmpNo=99999999", "regEmpName=test9999", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0128d14dcecb49189f5094c4fee85d76')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng466bJob,
        Complete
    ]) 

    # authCheck >> csng466bJob >> Complete
    workflow








